package me.lpmg.ste.data

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.s
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import java.nio.file.Path
import java.time.LocalDateTime
import java.time.ZonedDateTime

/** This class provides functionality to manage revisions and revision pairs.
  *
  * @param spark
  *   The Spark session to use.
  * @param dataFolderPath
  *   The path to the data f‚older.
  */
class RevisionManager(
    spark: SparkSession,
    dataFolderPath: String
) {
  val logger = Logger(getClass.getName)

  def retrieveRevisions(
      filesRDD: RDD[(String, PortableDataStream)],
      template: String
  ): RDD[Revision] = {
    filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionsFromPDS(pds, template)
      }
  }

  def retrieveRevisionPairs(
      filesRDD: RDD[(String, PortableDataStream)],
      template: String
  ): RDD[RevisionPair] = {
    filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionPairsFromPDS(pds, template)
      }
  }

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

  def saveRevisionPairsToFile(
      revisionPairs: RDD[RevisionPair],
      outputFolder: String
  ): Unit = {
    import spark.implicits._

    val outputPath = Path.of(dataFolderPath).resolve(outputFolder)
    if (!outputPath.toFile.exists()) {
      outputPath.toFile.mkdirs()
    }

    val serializedRevisionPairs = revisionPairs
      .map { case (rev: RevisionPair) =>
        (
          rev.revisionIdTemplateAdded,
          rev.revisionIdTemplateRemoved,
          rev.pageId,
          rev.sourcesTemplateAdded,
          rev.sourcesTemplateRemoved
        )
      }
      .toDF(
        "revIdTA",
        "revIdTR",
        "pageId",
        "srcTA",
        "srcTR"
      )

    serializedRevisionPairs.write
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

      val positiveLabelThreshold = 0.9999f
      new Revision(
        row.getAs[Long]("revId"),
        row.getAs[Long]("pairId"),
        row.getAs[Int]("pageId"),
        templateAdded >= positiveLabelThreshold,
        templateRemoved >= positiveLabelThreshold,
        templateAddedGT >= positiveLabelThreshold,
        templateRemovedGT >= positiveLabelThreshold,
        row.getAs[Seq[String]]("src")
      )
    }
  }

  def loadRevisionPairs(inputFolder: String): RDD[RevisionPair] = {
    import spark.implicits._

    val inputPath = Path.of(dataFolderPath).resolve(inputFolder)
    if (!inputPath.toFile.exists()) {
      throw new IllegalArgumentException(
        s"Input folder does not exist: $inputPath"
      )
    }

    // Read the parquet file
    val serializedRevisionPairsDF = spark.read.parquet(inputPath.toString())

    // Convert back to RDD[RevisionPair]
    serializedRevisionPairsDF.rdd.map { row =>
      new RevisionPair(
        row.getAs[Long]("revIdTA"),
        row.getAs[Long]("revIdTR"),
        row.getAs[Int]("pageId"),
        row.getAs[Seq[String]]("srcTA"),
        row.getAs[Seq[String]]("srcTR")
      )
    }
  }
}
