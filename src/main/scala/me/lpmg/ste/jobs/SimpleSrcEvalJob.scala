package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.{col, expr, when}
import scala.collection.mutable
import me.lpmg.ste.algorithms.SimpleSourceEvaluator
import java.nio.file.Path
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.types.Types.TemplateBitPositions
import org.apache.spark.sql.Row
import me.lpmg.ste.types.TemplateProbabilityVector
import me.lpmg.ste.algorithms.ProbabilityHandler

object SimpleSrcEvalJob {

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
    val revisionOutputLimit: Long = if (args.length > 2) args(2).toLong else 0

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions = revisionManager.loadRevisions(revisionsFolderName)

    // SOURCE SCORE COMPUTATION
    val templateSourceScores = TemplateBitPositions.map {
      case (template, position) =>
        val sourceScores = SimpleSourceEvaluator.evaluateSources(
          revisions,
          position
        )
        (template -> sourceScores)
    }.toMap

    // Print source scores for debugging
    templateSourceScores.foreach { case (template, sourceScores) =>
      logger.info(s"Template: $template")
      sourceScores.foreach { case (source, score) =>
      logger.info(s"Source: $source, Score: $score")
      }
    }

    val templateProbabilities = revisions
      .map { revision =>
        var probabilitiesMap: mutable.Map[String, TemplateProbabilityVector] =
          mutable.Map().empty
        templateSourceScores.foreach { case (template, sourceScores) =>
          val probabilities = revision.sources
            .map(source =>
              sourceScores
                .getOrElse(source, TemplateProbabilityVector(0.0f, 0.0f, 0.0f))
            )
            .toSeq
          val accumulatedProbabilities =
            ProbabilityHandler.softmaxAggregation(probabilities, 0.7f)

          probabilitiesMap.put(template, accumulatedProbabilities)
        }
        (revision.revisionId, probabilitiesMap)
      }
      .filter(_._1 >= revisionOutputLimit)

    // make predictions for specific use case
    val templatePredictions = templateProbabilities.map {
      case (revisionId, probabilities) =>
        val predictions = probabilities
          .map { case (template, probabilityVector) =>
            val prediction =
              if (
                probabilityVector.probabilityTemplateAdded > probabilityVector.probabilityTemplateRemoved
              ) 1.0f
              else 0.0f
            (template, prediction)
          }
          .filter(_._2 > 0.0001f)
        (revisionId, predictions)
    }.filter(_._2.isEmpty == false)

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    val templateNames = TemplateBitPositions.keySet.toSeq

    // REVISION SCORES
    val revisionScoresOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"template-prediction-$dateString")
    val revisionScoresRows = templatePredictions.map {
      case (revisionId, scores) =>
        val rowValues =
          templateNames.map(key => scores.getOrElse(key, null))
        Row.fromSeq(revisionId +: rowValues)
    }
    val schema = StructType(
      StructField("revision_id", LongType, nullable = false) +:
        templateNames.map(key =>
          StructField(
            s"pred_${key.toLowerCase.replaceAll(" ", "-")}",
            FloatType,
            nullable = true
          )
        )
    )

    val revisionScoresDF = spark.createDataFrame(revisionScoresRows, schema)

    val formattedColumns = templateNames.map { key =>
      bround(col(s"rs_${key.toLowerCase.replaceAll(" ", "-")}"), 2)
        .alias(s"rs_${key.toLowerCase.replaceAll(" ", "-")}")
    }
    val formattedDF =
      revisionScoresDF.select(col("revision_id") +: formattedColumns: _*)

    formattedDF.write
      .mode("overwrite")
      .option("header", "true")
      .option("nullValue", "")
      .csv(revisionScoresOutputPath.toString)

    logger.warn(
      s"CSV file saved: ${revisionScoresOutputPath.toString()}"
    )

    spark.stop()
  }

  private def getBit(templateName: String): Byte = {
    TemplateBitPositions.getOrElse(templateName, 0.toByte)
  }
}
