package me.lpmg.ste.data

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.types.Types.{
  bitSetToByteArray,
  byteArrayToBitSet,
  TemplateBitPositions
}
import me.lpmg.ste.time.Watch
import java.nio.file.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.Revision
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZonedDateTime
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.util.collection.BitSet
import org.apache.spark.graphx.Edge
import me.lpmg.ste.types.EdgeType
import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.Types
import org.apache.spark.storage.StorageLevel
import org.apache.spark.input.PortableDataStream
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.s

class RevisionManager(
    spark: SparkSession,
    dumpFolderPath: String,
    dataFolderPath: String
) {
  val logger = Logger(getClass.getName)
  var dateLimit: Long = 0

  /** Sets the date limit for the graph. Only revisions after this date will be
    * included in the graph.
    *
    * @param date
    */
  def setDateLimit(date: ZonedDateTime): Unit = {
    setDateLimit(date.toInstant().toEpochMilli())
  }

  /** Sets the date limit for the graph. Only revisions after this date will be
    * included in the graph.
    *
    * @param date
    */
  def setDateLimit(date: Long): Unit = {
    dateLimit = date
  }

  /** Initializes the graph by reading all revisions from the dump folder.
    *
    * @return
    *   The revision graph
    */
  def retrieveRevisions(
      filesRDD: RDD[(String, PortableDataStream)]
  ): RDD[Revision] = {

    /////////////////////////////////////////////////////////////////////////////////////////
    // REVISION EXTRACTION
    /////////////////////////////////////////////////////////////////////////////////////////
    val fixedDateLimit = dateLimit
    val allRevisionsRDD = filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionsFromPDS(
          pds,
          fixedDateLimit
        )
      }
    allRevisionsRDD
    // Find oldest revisions and update parent IDs in a distributed way
    // val updatedRevisionsRDD = if (fixedDateLimit <= 0) {
    //   allRevisionsRDD
    // } else {
    //   logger.warn("Setting parent ID of oldest revisions to -1")

    //   // Group revisions by their page ID and sort by timestamp
    //   val groupedRevisionsRDD =
    //     allRevisionsRDD
    //       .groupBy(_.pageId)
    //       .mapValues { revisions =>
    //         // [oldest, ..., newest]
    //         revisions.toSeq.sortBy(_.timestamp).map { rev =>
    //           rev.toIdTimestampPair
    //         }
    //       }

    //   // Create an RDD of oldest revision IDs with a marker
    //   val oldestRevisionsRDD = groupedRevisionsRDD
    //     .flatMap { case (pageId, revisions) =>
    //       revisions.headOption.map(rev => (rev._1, true))
    //     }

    //   // Use leftOuterJoin to mark oldest revisions
    //   allRevisionsRDD
    //     .keyBy(_.revisionId) // Create key-value pairs for join
    //     .leftOuterJoin(oldestRevisionsRDD)
    //     .map { case (revId, (revision, isOldest)) =>
    //       if (isOldest.isDefined) {
    //         revision.copy(parentId = -1)
    //       } else {
    //         revision
    //       }
    //     }
    // }
  }

  /**
    * Saves the revisions to a parquet file.
    *
    * @param revisions
    * @param outputFolder
    */
  def saveRevisionsToFile(
      revisions: RDD[Revision],
      outputFolder: String
  ): Unit = {
    import spark.implicits._

    val outputPath = Path.of(dataFolderPath).resolve(outputFolder)
    if (!outputPath.toFile.exists()) {
      outputPath.toFile.mkdirs()
    }

    // Convert BitSets to byte arrays for storage
    val serializedRevisions = revisions
      .map { case (rev: Revision) =>
        val templatePresenceBytes =
          bitSetToByteArray(rev.templatePresence)
        val templateAddedBytes = bitSetToByteArray(rev.templateAdded)
        val templateRemovedBytes = bitSetToByteArray(rev.templateRemoved)
        (
          rev.revisionId,
          rev.pageId,
          rev.parentId,
          rev.timestamp,
          rev.contributorId,
          templatePresenceBytes,
          templateAddedBytes,
          templateRemovedBytes,
          rev.sources
        )
      }
      .toDF(
        "revisionId",
        "pageId",
        "parentId",
        "timestamp",
        "contributorId",
        "templatePresence",
        "templateAdded",
        "templateRemoved",
        "sources"
      )

    serializedRevisions.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(outputPath.toString())
  }

  /** Saves revisions that have at least one template added or removed.
    *
    * @param revisions
    *   RDD containing the revisions to filter and save
    * @param outputPath
    *   Path where the filtered revisions should be saved
    * @return
    *   The number of saved revisions
    */
  def saveRevisionsWithTemplateChanges(
      revisions: RDD[Revision],
      outputFolder: String
  ): Unit = {
    val filteredRevisions = revisions.filter(revision =>
      revision.templateAdded.cardinality() > 0 || revision.templateRemoved
        .cardinality() > 0
    )
    saveRevisionsToFile(filteredRevisions, outputFolder)
  }

  /** Loads revisions from a parquet file.
    *
    * @param inputFolder
    *   Folder name where the revisions were saved
    * @return
    *   RDD[Revision] containing the loaded revisions
    */
  def loadRevisions(
      inputFolder: String
  ): RDD[Revision] = {
    import spark.implicits._

    val inputPath = Path.of(dataFolderPath).resolve(inputFolder)
    if (!inputPath.toFile.exists()) {
      throw new IllegalArgumentException(
        s"Input folder does not exist: $inputPath"
      )
    }

    // Read the parquet file
    val serializedRevisionsDF = spark.read.parquet(inputPath.toString())

    // Convert back to RDD[Revision]
    serializedRevisionsDF.rdd.map { row =>
      val templatePresence = byteArrayToBitSet(
        row.getAs[Array[Byte]]("templatePresence"),
        Types.TemplateBitPositions.size
      )
      val templateAdded = byteArrayToBitSet(
        row.getAs[Array[Byte]]("templateAdded"),
        Types.TemplateBitPositions.size
      )
      val templateRemoved = byteArrayToBitSet(
        row.getAs[Array[Byte]]("templateRemoved"),
        Types.TemplateBitPositions.size
      )

      new Revision(
        row.getAs[Long]("revisionId"),
        row.getAs[Int]("pageId"),
        row.getAs[Long]("parentId"),
        row.getAs[Long]("timestamp"),
        row.getAs[Int]("contributorId"),
        templatePresence,
        templateAdded,
        templateRemoved,
        row.getAs[Seq[String]]("sources")
      )
    }
  }
}
