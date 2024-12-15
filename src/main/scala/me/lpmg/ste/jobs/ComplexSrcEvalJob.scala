package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import java.time.ZonedDateTime
import java.time.ZoneId
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.RevisionManager
import me.lpmg.ste.graph.GraphCreator
import me.lpmg.ste.graph.RevisionVertex
import me.lpmg.ste.graph.SourceVertex

object ComplexSrcEvalJob {
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
    val graph = GraphCreator.createRevisionGraph(revisions)

    // PAGERANK
    val ranks = graph.pageRank(tol = 0.01).vertices
    val updatedVertices = graph.vertices.leftJoin(ranks) {
      case (id, vertex, Some(rank)) =>
        vertex match {
          case r: RevisionVertex => r.copy(trustScore = rank.toFloat)
          case s: SourceVertex   => s.copy(trustScore = rank.toFloat)
        }
      case (id, vertex, None) =>
        vertex // If rank is missing, keep original vertex
    }
  }

}
