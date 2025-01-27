package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import java.time.ZonedDateTime
import java.time.ZoneId
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.RevisionManager
import java.nio.file.Path
import me.lpmg.ste.types.TemplateProbabilityVector
import me.lpmg.ste.types.Types.TemplateBitPositions
import me.lpmg.ste.algorithms.ProbabilityHandler
import scala.collection.mutable
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision

object CountUnknownSourcesJob {
  val DefaultTemplateProbabilityVector = TemplateProbabilityVector(0.5f, 0.5f)

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    if (args.length < 1) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the revisions folder")
      System.exit(1)
    } else if (args.length < 3) {
      logger.error("Please specify the source probabilities folder")
      System.exit(1)
    } else if (args.length < 4) {
      logger.error("Please specify the template")
      System.exit(1)
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)
    val sourceProbabilitiesFolderName = args(2)
    val template = args(3)
    val escapedTemplate = template.toLowerCase().replace(" ", "-")

    var testSplitRevision = 0L
    if (args.length > 4) {
      try {
        testSplitRevision = args(4).toLong
      } catch {
        case e: NumberFormatException =>
          logger.error("Invalid test split revision number")
          System.exit(1)
      }
    }

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    // only load test split revisions for evaluation
    val revisions: RDD[Revision] = revisionManager
      .loadRevisions(revisionsFolderName)
      .filter(_.revisionId >= testSplitRevision)

    val sourceProbabilitiesFolderPath =
      Path.of(dataFolderPath).resolve(sourceProbabilitiesFolderName)
    if (!sourceProbabilitiesFolderPath.toFile.exists()) {
      throw new IllegalArgumentException(
        s"Input folder does not exist: $sourceProbabilitiesFolderPath"
      )
    }

    val sourceProbabilitiesDF = spark.read
      .option("header", "true")
      .csv(sourceProbabilitiesFolderPath.toString)

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // source -> TemplateProbabilityVector
    val sourceProbabilitiesMap = sourceProbabilitiesDF
      .rdd
      .map { row =>
        val source = row.getAs[String]("src")
        val probabilityTemplateAdded = row.getAs[String]("probabilityTemplateAdded").toFloat
        // only intialize with template added probability to ensure it adds up to 1.0f exactly
        //val probabilityTemplateRemoved = row.getAs[String]("probabilityTemplateRemoved").toFloat
        val occurences = row.getAs[String]("occurences").toInt

        (source, (TemplateProbabilityVector(probabilityTemplateAdded), occurences))
      }
      .collectAsMap()

    val totalRevisionsCount = revisions.count()
    val revisionsWithUnknownSources = revisions
      .map { revision =>
        val unknownSources = revision.sources.count(source => !sourceProbabilitiesMap.contains(source))
        val allSources = revision.sources.size
        (unknownSources, allSources)
      }
      .filter { case (unknownSources, _) => unknownSources > 0 }
      .cache()

    val countRevisionsWithUnknownSources = revisionsWithUnknownSources.count()
    val averageUnknownSourceRatio = revisionsWithUnknownSources
      .map { case (unknownSources, allSources) => unknownSources.toDouble / allSources }
      .mean()

    val percentageRevisionsWithUnknownSources = (countRevisionsWithUnknownSources.toDouble / totalRevisionsCount) * 100

    logger.info(s"Total number of revisions: $totalRevisionsCount")
    logger.info(s"Number of revisions with at least one unknown source: $countRevisionsWithUnknownSources")
    logger.info(f"Percentage of revisions with at least one unknown source: $percentageRevisionsWithUnknownSources%.2f%%")
    logger.info(f"Average ratio of unknown sources to all sources: $averageUnknownSourceRatio%.2f")

    spark.stop()
  }

}
