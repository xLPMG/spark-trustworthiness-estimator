package me.lpmg.ste.data

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.types.Types.{
  bitSetToString,
  stringToBitSet,
  TemplateBitPositions
}
import me.lpmg.ste.time.Watch
import java.nio.file.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.graphx.Graph
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZonedDateTime
import org.apache.spark.util.collection.BitSet
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.Types
import org.apache.spark.storage.StorageLevel
import org.apache.spark.input.PortableDataStream
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.s

class RevisionManager(
    spark: SparkSession,
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
  }

  /** Saves the revisions to a parquet file.
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
        val templatePresenceString =
          bitSetToString(rev.templatePresence)
        val templateAddedString = bitSetToString(rev.templateAdded)
        val templateRemovedString = bitSetToString(rev.templateRemoved)

        val templatePresenceGTString = bitSetToString(rev.templatePresenceGT)
        val templateAddedGTString = bitSetToString(rev.templateAddedGT)
        val templateRemovedGTString = bitSetToString(rev.templateRemovedGT)

        (
          rev.revisionId,
          rev.pageId,
          rev.parentId,
          rev.timestamp,
          templatePresenceString,
          templateAddedString,
          templateRemovedString,
          templatePresenceGTString,
          templateAddedGTString,
          templateRemovedGTString,
          rev.sources
        )
      }
      .toDF(
        "revId",
        "pageId",
        "parentId",
        "timestamp",
        "tP",
        "tA",
        "tR",
        "tP_GT",
        "tA_GT",
        "tR_GT",
        "src"
      )

    serializedRevisions.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(outputPath.toString())
  }

  def loadRevisions(
      inputFolder: String,
      hasGTData: Boolean = true
  ): RDD[Revision] = {
    if (hasGTData) {
      logger.info("Loading revisions with Ground Truth data")
    } else {
      logger.info("Loading revisions without Ground Truth data")
    }

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
      val templatePresence = stringToBitSet(
        row.getAs[String]("tP"),
        Types.TemplateBitPositions.size
      )
      val templateAdded = stringToBitSet(
        row.getAs[String]("tA"),
        Types.TemplateBitPositions.size
      )
      val templateRemoved = stringToBitSet(
        row.getAs[String]("tR"),
        Types.TemplateBitPositions.size
      )

      // GT
      val templatePresenceGT =
        if (!hasGTData) templatePresence
        else
          stringToBitSet(
            row.getAs[String]("tP_GT"),
            Types.TemplateBitPositions.size
          )

      val templateAddedGT =
        if (!hasGTData) templateAdded
        else
          stringToBitSet(
            row.getAs[String]("tA_GT"),
            Types.TemplateBitPositions.size
          )

      val templateRemovedGT =
        if (!hasGTData) templateRemoved
        else
          stringToBitSet(
            row.getAs[String]("tR_GT"),
            Types.TemplateBitPositions.size
          )

      new Revision(
        row.getAs[Long]("revId"),
        row.getAs[Int]("pageId"),
        row.getAs[Long]("parentId"),
        row.getAs[Long]("timestamp"),
        templatePresence,
        templateAdded,
        templateRemoved,
        templatePresenceGT,
        templateAddedGT,
        templateRemovedGT,
        row.getAs[Seq[String]]("src")
      )
    }
  }
}
