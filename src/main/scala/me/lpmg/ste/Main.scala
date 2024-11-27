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

    val graphManager = new GraphManager(spark, dumpFolderPath, dataFolderPath)
    graphManager.setDateLimit(
      ZonedDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"))
    )
    val revisionGraph = graphManager.initializeGraph()

    logger.warn(s"Graph vertices: ${revisionGraph.vertices.count()}")
    logger.warn(s"Graph edges: ${revisionGraph.edges.count()}")

    // graphManager.saveGraph("editedGraph", editedGraph)

    // trust computation
    // val initialGraph = TrustCalculator.initializeTrustScores(revisionGraph)
    // val trustGraph = TrustCalculator.computeTrustRank(initialGraph, spark)

    // initialGraph.vertices.collect().foreach { case (id, vertex) =>
    //   println(s"Vertex ID: $id, Data: $vertex")
    // }
    // logger.warn("after trust")
    // trustGraph.vertices.collect().foreach { case (id, vertex) =>
    //   println(s"Vertex ID: $id, Data: $vertex")
    // }

    // trustGraph.edges.collect().foreach { edge =>
    //   println(s"Edge: ${edge.srcId} -> ${edge.dstId}, Attr: ${edge.attr}")
    // }

    spark.stop()
    logger.warn(s"Total Time: ${Watch.stopFormatted("Main")}")
  }

}
