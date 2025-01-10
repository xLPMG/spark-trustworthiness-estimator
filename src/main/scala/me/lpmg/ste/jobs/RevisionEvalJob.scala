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
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)
    val sourceProbabilitiesFolderName = args(2)
    val revisionOutputLimit: Long = if (args.length > 3) args(3).toLong else 0

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions = revisionManager.loadRevisions(revisionsFolderName)

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

    // template -> [source -> probabilities]
    val templateToSourceProbabilitiesMap = sourceProbabilitiesDF
      .flatMap { row =>
        val source = row.getAs[String]("src")
        row.schema.fieldNames.filter(_ != "src").map { template =>
          val templateName = template.replace("-", " ")
          val probabilitiesString = row.getAs[String](template)

          val probabilityVector =
            if (null == probabilitiesString || probabilitiesString.isEmpty) {
              DefaultTemplateProbabilityVector
            } else {
              val pA = probabilitiesString
                .stripPrefix("(")
                .stripSuffix(")")
                .split(";")
                .map(_.toFloat)
              TemplateProbabilityVector(pA(0), pA(1))
            }

          (templateName, source, probabilityVector)
        }
      }
      .rdd
      .groupBy(_._1)
      .mapValues(_.map { case (_, source, templateProbabilityVector) =>
        (source, templateProbabilityVector)
      }.toMap)
      .collectAsMap()

    val revisionToTemplateProbabilities = revisions
      .map { revision =>
        // template -> probabilities
        var probabilitiesMap: mutable.Map[String, TemplateProbabilityVector] =
          mutable.Map().empty

        templateToSourceProbabilitiesMap.foreach {
          case (template, sourceToProbabilities) =>
            // probabilities for each source
            val probabilities = revision.sources
              .map(source =>
                sourceToProbabilities
                  .getOrElse(
                    source,
                    DefaultTemplateProbabilityVector
                  )
              )
              .toSeq

            // TODO: implement
            val accumulatedProbabilities =
              ProbabilityHandler.naiveBayesProduct(probabilities)

            probabilitiesMap.put(template, accumulatedProbabilities)
        }
        (revision.revisionId, probabilitiesMap)
      }

    // PREDICTIONS for has_template
    val templatePredictions = revisionToTemplateProbabilities
      .map { case (revisionId, probabilities) =>
        val predictions = probabilities
          .map { case (template, probabilityVector) =>
            val prediction =
              if (
                probabilityVector.probabilityTemplateAdded > probabilityVector.probabilityTemplateRemoved
              ) 1.0f
              else null
            (template, prediction)
          }
        (revisionId, predictions)
      }

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    val templateNames = TemplateBitPositions.keySet.toSeq

    // REVISION SCORES
    val predictionsOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"template-predictions-$dateString")
    val revisionScoresRows = templatePredictions.map {
      case (revisionId, scores) =>
        val rowValues =
          templateNames.map(key => scores.getOrElse(key, null))
        Row.fromSeq(revisionId +: rowValues)
    }
    // tail -> columns except revision_id
    // filter out rows with all null values
    .filter(row => row.toSeq.tail.exists(_ != null))

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

    val predictionsDF = spark.createDataFrame(revisionScoresRows, schema)

    predictionsDF.write
      .mode("overwrite")
      .option("header", "true")
      .option("nullValue", "")
      .csv(predictionsOutputPath.toString)

    logger.warn(
      s"CSV file saved: ${predictionsOutputPath.toString()}"
    )

    spark.stop()
  }

}
