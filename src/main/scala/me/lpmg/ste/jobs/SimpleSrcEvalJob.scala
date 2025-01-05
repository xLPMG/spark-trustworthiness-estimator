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
        val minimumValue = 0.001f

        // TODO: check for performance upgrades (iterate sources once)
        // for each template
        templateSourceScores.foreach { case (template, sourceScores) =>
          // sum up source scores for all sources of the revision
          val likelihood = revision.sources
            .map(source => sourceScores.getOrElse(source, 0.0f))
            .sum

          val sigLikelihood = sig(likelihood)
          if (
            sigLikelihood > 0.5f + minimumValue || sigLikelihood < 0.5f - minimumValue
          ) {
            likelihoodMap.put(template, sigLikelihood)
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

    // LIKELIHOODS
    val likelihoodsOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"simple-template-likelihoods-$dateString")
    val likelihoodRows = templateLikelihoods.map {
      case (revisionId, likelihoods) =>
        val rowValues =
          templateNames.map(key => likelihoods.getOrElse(key, null))
        Row.fromSeq(revisionId +: rowValues)
    }
    val schema = StructType(
      StructField("revision_id", LongType, nullable = false) +:
        templateNames.map(key =>
          StructField(
            s"lh_${key.toLowerCase.replaceAll(" ", "_")}",
            FloatType,
            nullable = true
          )
        )
    )

    val likelihoodsDF = spark.createDataFrame(likelihoodRows, schema)

    val formattedColumns = templateNames.map { key =>
      format_number(col(s"lh_${key.toLowerCase.replaceAll(" ", "_")}"), 4)
        .alias(s"lh_${key.toLowerCase.replaceAll(" ", "_")}")
    }
    val formattedDF =
      likelihoodsDF.select(col("revision_id") +: formattedColumns: _*)

    formattedDF.write
      .mode("overwrite")
      .option("header", "true")
      .option("nullValue", "")
      .csv(likelihoodsOutputPath.toString)

    logger.warn(
      s"CSV file saved: ${likelihoodsOutputPath.toString()}"
    )

    // LABELS
    val labelsOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"simple-template-labels-$dateString")

    val revisionsWithTemplate =
      revisions.filter(_.templatePresenceGT.cardinality() > 0)
    val labelsRows = revisionsWithTemplate.map { revision =>
      val rowValues = templateNames.map { template =>
        if (revision.templatePresenceGT.get(getBit(template))) 1.0f else null
      }.toSeq
      Row.fromSeq(revision.revisionId +: rowValues)
    }

    val labelsSchema = StructType(
      StructField("revision_id", LongType, nullable = false) +:
        templateNames.map(template =>
          StructField(
            s"gt_${template.toLowerCase.replaceAll(" ", "_")}",
            FloatType,
            nullable = true
          )
        )
    )

    val labelsDF = spark.createDataFrame(labelsRows, labelsSchema)

    labelsDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(labelsOutputPath.toString)

    logger.warn(
      s"CSV file saved: ${labelsOutputPath.toString()}"
    )

    spark.stop()
  }

  private def getBit(templateName: String): Byte = {
    TemplateBitPositions.getOrElse(templateName, 0.toByte)
  }

  private def sig(value: Float): Float = {
    1.0f / (1.0f + math.exp(-value).toFloat)
  }
}
