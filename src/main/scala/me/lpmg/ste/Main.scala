package me.lpmg.ste

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.graph.GraphManager
import org.apache.spark.graphx.Graph
import me.lpmg.ste.data.Revision
import me.lpmg.ste.algorithms.TrustCalculator

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

    val spark = SparkSession
      .builder()
      .getOrCreate()

    val graphManager = new GraphManager(spark, dumpFolderPath, dataFolderPath)
    val revisionGraph = graphManager.initializeGraph()

    logger.warn(s"Graph vertices: ${revisionGraph.vertices.count()}")
    logger.warn(s"Graph edges: ${revisionGraph.edges.count()}")

    val positiveSeeds = Seq(213132L, 3634534L, 12212L)
    val negativeSeeds = Seq(23423L, 9845L, 34554L)
    val editedGraph = TrustCalculator.initTrustScores(
      revisionGraph,
      positiveSeeds,
      negativeSeeds,
      1.0f,
      0.0f
    )

    graphManager.saveGraph("editedGraph", editedGraph)

    spark.stop()
    logger.warn(s"Total Time: ${Watch.stopFormatted("Main")}")
  }

}
