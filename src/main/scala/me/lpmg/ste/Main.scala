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
    //   ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"))
    // )
    val revisions = revisionManager.retrieveRevisions()
    val revisionGraph = GraphCreator.createRevisionGraph(revisions)

    logger.warn(s"Graph vertices: ${revisionGraph.vertices.count()}")
    logger.warn(s"Graph edges: ${revisionGraph.edges.count()}")

    // trust computation
    val initialGraph = TrustCalculator.initializeTrustScores(revisionGraph)
    val trustGraph = TrustCalculator.computeTrustScores(initialGraph, spark)

    // source evaluation
    val sourcesRDD =
      SourceEvaluator.evaluateSourcesDistributed(
        revisions,
        Seq(0, 1, 2, 3, 4, 5, 6)
      )

    // contributor evaluation
    val contributorsRDD =
      ContributorEvaluator.evaluateContributorsDistributed(
        revisions,
        Seq(7, 8, 9, 10, 11, 12, 13, 14)
      )

    spark.stop()
    logger.warn(s"Total Time: ${Watch.stopFormatted("Main")}")
  }

}
