package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.{col, expr, when}
import scala.collection.mutable
import me.lpmg.ste.algorithms.SourceEvaluator
import java.nio.file.Path
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.types.Types.TemplateBitPositions
import org.apache.spark.sql.Row
import me.lpmg.ste.types.TemplateProbabilityVector
import me.lpmg.ste.algorithms.ProbabilityHandler

object SourceEvalJob {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    if (args.length < 1) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the revisions folder name")
      System.exit(1)
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions = revisionManager.loadRevisions(revisionsFolderName)

    // template -> [source -> probabilities]
    val templateToSourceProbabilities = TemplateBitPositions.map {
      case (template, position) =>
        val sourceProbabilities = SourceEvaluator.evaluateSources(
          revisions,
          position
        )

        (template -> sourceProbabilities)
    }.toMap

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    val templateNames =
      TemplateBitPositions.keySet.toSeq.map(_.toLowerCase.replace(" ", "-"))

    val sourceProbabilitiesOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"source-probabilities-$dateString")

    val sourceProbabilitiesDF = templateToSourceProbabilities
      .flatMap { case (template, sourceProbabilities) =>
        sourceProbabilities.map { case (source, probabilities) =>
          val probabilitiesString =
            if (probabilities.isUndecided) ""
            else probabilities.extractValuesString
          (source, template.toLowerCase.replace(" ", "-"), probabilitiesString)
        }
      }
      .toSeq
      .toDF("src", "template", "probabilities")

    val pivotedDF = sourceProbabilitiesDF
      .groupBy("src")
      .pivot("template", templateNames)
      .agg(first("probabilities"))

    // Replace empty strings with null
    val cleanedDF = pivotedDF.na.replace(pivotedDF.columns, Map("" -> null))

    // Filter out rows where all template columns are null
    val filteredDF =
      cleanedDF.filter(row => row.toSeq.drop(1).exists(_ != null))

    filteredDF.write
      .option("header", "true")
      .option("compression", "gzip")
      .csv(sourceProbabilitiesOutputPath.toString)

    spark.stop()
  }
}
