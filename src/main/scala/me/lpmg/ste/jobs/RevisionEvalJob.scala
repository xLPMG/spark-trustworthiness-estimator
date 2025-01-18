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

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions: RDD[Revision] =
      revisionManager.loadRevisions(revisionsFolderName)

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
    val templateNames =
      TemplateBitPositions.keySet.toSeq.map(_.toLowerCase.replace(" ", "-"))

    // template -> [source -> probabilities]
    val templateToSourceProbabilitiesMap = sourceProbabilitiesDF
      .flatMap { row =>
        val source = row.getAs[String]("src")
        row.schema.fieldNames.filter(_ != "src").map { template =>
          val templateName = template.toLowerCase.replace(" ", "-")
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
      // CALCULATE PROBABILITIES FOR EACH REVISION
      .flatMap { revision =>
        // template -> probabilities
        templateToSourceProbabilitiesMap.flatMap {
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

            if (probabilities.isEmpty) {
              None
            } else {
              val accumulatedProbabilities =
                ProbabilityHandler.logarithmicCombination(probabilities)
              Some((revision.revisionId, template, accumulatedProbabilities))
            }
        }
      }
      .filter(!_._3.isUndecided())
      .map { case (revisionId, template, probabilities) =>
        (revisionId, template, probabilities.extractValuesString)
      }

    val probabilitiesDF = revisionToTemplateProbabilities
      .toDF("revision_id", "template", "probability")

    val pivotedDF = probabilitiesDF
      .groupBy("revision_id")
      .pivot("template", templateNames)
      .agg(first("probability"))

    val probabilitiesOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"template-probabilities-$dateString")

    pivotedDF.write
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
