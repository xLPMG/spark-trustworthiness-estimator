package me.lpmg.ste.data

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
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

  /** Initializes the graph by reading all revisions from the dump folder.
    *
    * @return
    *   The revision graph
    */
  def retrieveRevisions(
      filesRDD: RDD[(String, PortableDataStream)],
      template: String
  ): RDD[Revision] = {
    filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionsFromPDS(pds, template)
      }
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

    val serializedRevisions = revisions
      .map { case (rev: Revision) =>
        (
          rev.revisionId,
          rev.pairId,
          rev.pageId,
          if (rev.templateAdded) "1.0" else "0.0",
          if (rev.templateRemoved) "1.0" else "0.0",
          if (rev.templateAddedGT) "1.0" else "0.0",
          if (rev.templateRemovedGT) "1.0" else "0.0",
          rev.sources
        )
      }
      .toDF(
        "revId",
        "pairId", 
        "pageId",
        "tA",
        "tR",
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
      val templateAdded = row.getAs[String]("tA").toFloat
      val templateRemoved = row.getAs[String]("tR").toFloat

      // GT
      val templateAddedGT =
        if (!hasGTData) templateAdded
        else row.getAs[String]("tA_GT").toFloat

      val templateRemovedGT =
        if (!hasGTData) templateRemoved
        else row.getAs[String]("tR_GT").toFloat

      new Revision(
        row.getAs[Long]("revId"),
        row.getAs[Long]("pairId"),
        row.getAs[Int]("pageId"),
        templateAdded == 1.0f,
        templateRemoved == 1.0f,
        templateAddedGT == 1.0f,
        templateRemovedGT == 1.0f,
        row.getAs[Seq[String]]("src")
      )
    }
  }
}
