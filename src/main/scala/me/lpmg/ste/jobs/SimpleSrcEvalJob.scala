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

    // Calculate likelihoods and filter out revisions with no likelihoods
    val templateLikelihoods = revisions
      .map { revision =>
        var likelihoodMap: mutable.Map[String, Float] = mutable.Map().empty

        // TODO: check for performance upgrades (iterate sources once)
        // for each template
        templateSourceScores.foreach { case (template, sourceScores) =>
          // sum up source scores for all sources of the revision
          // it doesn't matter how many good sources (negative scores) there are
          // since bad sources can always cause a template to be present
            val likelihood = revision.sources
            .map(source => sourceScores.getOrElse(source, 0.0f)) 
            .sum

          if (
            likelihood > 0.0f
          ) {
            likelihoodMap.put(template, likelihood)
          }
        }
        (revision.revisionId, likelihoodMap)
      }
      .filter { case (revisionId, likelihoodMap) =>
        likelihoodMap.nonEmpty
      }

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    val templateNames = TemplateBitPositions.keySet.toSeq

    // REVISION SCORES
    val revisionScoresOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"template-scores-$dateString")
    val revisionScoresRows = templateLikelihoods.map {
      case (revisionId, scores) =>
        val rowValues =
          templateNames.map(key => scores.getOrElse(key, null))
        Row.fromSeq(revisionId +: rowValues)
    }
    val schema = StructType(
      StructField("revision_id", LongType, nullable = false) +:
        templateNames.map(key =>
          StructField(
            s"rs_${key.toLowerCase.replaceAll(" ", "-")}",
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
