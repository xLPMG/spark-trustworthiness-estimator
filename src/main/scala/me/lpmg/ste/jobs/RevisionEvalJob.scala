package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.algorithms.ProbabilityHandler
import me.lpmg.ste.data.Revision
import me.lpmg.ste.data.RevisionManager
import me.lpmg.ste.data.TemplateProbabilityVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import java.nio.file.Path
import java.time.ZoneId
import java.time.ZonedDateTime
import scala.collection.mutable

/**
  * This job evaluates revisions based on their sources.
  * Arguments: <data-folder-path> <revisions-folder> <source-probabilities-folder> <template> <test-split-revision>
  */
object RevisionEvalJob {
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

    val revisionToProbabilities = revisions
      .map { revision =>
        // probabilities for each source
        val probabilities = revision.sources
          .map(source =>
            sourceProbabilitiesMap
              .getOrElse(source, (DefaultTemplateProbabilityVector, 0))
          )

        if (probabilities.isEmpty) {
          (revision.revisionId, DefaultTemplateProbabilityVector)
        } else {
          val accumulatedProbabilities =
            ProbabilityHandler.weightedCombinationNoUnsureResults(probabilities)
          (revision.revisionId, accumulatedProbabilities)
        }
      }
      // Map probability values to string representation
      .map { case (revisionId, probabilities) =>
        val (probabilityTemplateAdded, probabilityTemplateRemoved) = probabilities.extractValuesString
        (revisionId, probabilityTemplateAdded, probabilityTemplateRemoved)
      }

    val probabilitiesDF =
      revisionToProbabilities.toDF("revision_id", "probabilityTemplateAdded", "probabilityTemplateRemoved")

    val probabilitiesOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"probabilities-$escapedTemplate-$dateString")

    probabilitiesDF.write
      .mode("overwrite")
      .option("header", "true")
      .option("nullValue", "")
      .csv(probabilitiesOutputPath.toString)

    logger.warn(
      s"CSV file saved: ${probabilitiesOutputPath.toString()}"
    )

    spark.stop()
  }

}
