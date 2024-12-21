package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.algorithms.SimpleSourceEvaluator
import java.nio.file.Path
import java.time.ZonedDateTime
import java.time.ZoneId

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
    var testSplit: Long = 0L
    if (args.length > 2) {
      testSplit = args(2).toLong
    }

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions = revisionManager.loadRevisions(revisionsFolderName, false)

    // calculate source scores
    val sourceSpecificSourceScores = SimpleSourceEvaluator.evaluateSources(
      revisions,
      Seq(0, 1, 2, 3, 4, 5, 6)
    )

    val generalSourceScores = SimpleSourceEvaluator.evaluateSources(
      revisions,
      Seq(7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    )

    // Calculate total score for each revision by summing up source scores
    // revisions with scores close to 0 are filtered out
    val sourceSpecificRevisionScores = revisions
      .map { revision =>
        val totalScore = revision.sources
          .map(source => sourceSpecificSourceScores.getOrElse(source, 0.0f))
          .sum
        (revision.revisionId, totalScore)
      }
      .filter { case (_, score) => score > 0.001f || score < -0.001f }

    val generalRevisionScores = revisions
      .map { revision =>
        val totalScore = revision.sources
          .map(source => generalSourceScores.getOrElse(source, 0.0f))
          .sum
        (revision.revisionId, totalScore)
      }
      .filter { case (_, score) => score > 0.001f || score < -0.001f }

    // Convert RDDs to DataFrames and save as single CSV
    import spark.implicits._

    val sourceScoresOutputPath =
      Path.of(dataFolderPath).resolve(s"simple-source-scores-$dateString")
    val specificPath = sourceScoresOutputPath.resolve("specific")
    val generalPath = sourceScoresOutputPath.resolve("general")

    sourceSpecificRevisionScores
      .toDF("revision_id", "source_specific_score")
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(specificPath.toString())

    generalRevisionScores
      .toDF("revision_id", "general_score")
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(generalPath.toString())

    logger.warn(
      s"CSV file saved: ${sourceScoresOutputPath.toString()} with headers: revision_id,source_specific_score,general_score"
    )

    spark.stop()
  }

}
