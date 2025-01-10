package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import java.time.ZonedDateTime
import java.time.ZoneId
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.RevisionManager
import me.lpmg.ste.graph.GraphCreator
import me.lpmg.ste.graph.RevisionVertex
import me.lpmg.ste.graph.SourceVertex
import me.lpmg.ste.algorithms.ComplexSourceEvaluator
import org.apache.spark.graphx.Graph
import java.nio.file.Path

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

    //TODO: handle null type vertices 

    // Pregel
    val initializedGraph = Graph(
      ComplexSourceEvaluator.initializeVertices(graph.vertices, 0),
      graph.edges
    )

    val pregelVertices =
      ComplexSourceEvaluator.runPregel(initializedGraph).vertices

    // PAGERANK
    val ranks = graph.pageRank(tol = 0.01).vertices

    // SAVE DATA
    import spark.implicits._

    val sourceScoresOutputPath =
      Path.of(dataFolderPath).resolve(s"complex-source-scores-$dateString")

    pregelVertices
      .flatMap {
        case (id, rev: RevisionVertex) =>
          // filter out scores close to zero
          if (rev.trustScore > 0.001f || rev.trustScore < -0.001f) {
            Some((rev.id, rev.trustScore))
          } else {
            None
          }
        case _ =>
          None
      }
      .toDF("revision_id", "pregel_score")
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(sourceScoresOutputPath.resolve("pregel").toString())

    ranks
      .flatMap {
        case (id, rank) =>
          // filter out source ids and scores close to zero
          if (id > 0 && (rank > 0.001f || rank < -0.001f)) {
            Some((id, rank))
          } else {
            None
          }
        case _ =>
          None
      }
      .toDF("revision_id", "pagerank_score")
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv(sourceScoresOutputPath.resolve("page_rank").toString())

    logger.warn(
      s"CSV file saved: ${sourceScoresOutputPath.toString()} with headers: revision_id,source_specific_score,general_score"
    )

    spark.stop()
  }

}
