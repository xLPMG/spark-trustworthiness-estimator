package me.lpmg.ste

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.graph.GraphManager
import org.apache.spark.graphx._
import me.lpmg.ste.types.Revision
import me.lpmg.ste.algorithms.TrustCalculator
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.types.RevisionVertex
import me.lpmg.ste.algorithms.ContributorEvaluator
import me.lpmg.ste.data.RevisionManager
import me.lpmg.ste.graph.GraphCreator
import javax.xml.transform.Source
import me.lpmg.ste.algorithms.SourceEvaluator
import java.nio.file.Path
import org.apache.spark.storage.StorageLevel

object Main {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)
    Watch.start("Main")
    if (args.length < 1) {
      logger.error("Please specify the dump folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    }

    val dumpFolderPath = args(0)
    val dataFolderPath = args(1)

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dumpFolderPath, dataFolderPath)

    // revisionManager.setDateLimit(
    //   ZonedDateTime.of(
    //     2021,
    //     1,
    //     1,
    //     0,
    //     0,
    //     0,
    //     0,
    //     ZoneId.of("UTC")
    //   )
    // )

    // Read all .xml.bz2 files in the folder into an RDD
    val filesRDD = spark.sparkContext.binaryFiles(s"$dumpFolderPath/*.bz2")
    val filesCount = filesRDD.count()
    logger.warn(s"Total files found: ${filesCount}")

    val revisions = revisionManager.retrieveRevisions(filesRDD)
    revisions.repartition(filesCount.toInt)

    val numsaved = revisionManager.saveRevisionsWithTemplateChanges(
      revisions,
      "revisions_with_template_changes_test"
    )
    logger.warn(s"Saved $numsaved revisions with template changes")

    val loadedRevisions =
      revisionManager
        .loadRevisionsWithTemplateChanges(
          "revisions_with_template_changes_test"
        )
        .persist(StorageLevel.MEMORY_AND_DISK)

    // Check if we have any revisions
    val revisionCount = loadedRevisions.count()
    logger.warn(s"Loaded $revisionCount revisions with template changes")

    // calculate source scores
    val sourceSpecificSourceScores = SourceEvaluator.evaluateSources(
      loadedRevisions,
      Seq(0, 1, 2, 3, 4, 5, 6)
    )

    val generalSourceScores = SourceEvaluator.evaluateSources(
      loadedRevisions,
      Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    )

    // Calculate total score for each revision by summing up source scores
    val sourceSpecificRevisionScores = loadedRevisions.map { revision =>
      val totalScore = revision.sources
        .map(source => sourceSpecificSourceScores.getOrElse(source, 0.0f))
        .sum
      (revision.revisionId, totalScore)
    }

    val generalRevisionScores = loadedRevisions.map { revision =>
      val totalScore = revision.sources
        .map(source => generalSourceScores.getOrElse(source, 0.0f))
        .sum
      (revision.revisionId, totalScore)
    }

    // Convert RDDs to DataFrames and save as single CSV
    import spark.implicits._

    val sourceScoresOutputPath =
      Path.of(dataFolderPath).resolve("revision_scores")

    sourceSpecificRevisionScores
      .toDF("revision_id", "source_specific_score")
      .join(
        generalRevisionScores.toDF("revision_id", "general_score"),
        Seq("revision_id")
      )
      .coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(sourceScoresOutputPath.toString())

    logger.warn(
      s"CSV file saved: ${sourceScoresOutputPath.toString()} with headers: revision_id,source_specific_score,general_score"
    )

    spark.stop()
    logger.warn(s"Total Time: ${Watch.stopFormatted("Main")}")
  }

}
