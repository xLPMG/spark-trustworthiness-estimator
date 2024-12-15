package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import me.lpmg.ste.graph.RevisionVertex
import me.lpmg.ste.graph.EdgeType

object ContributorEvaluator extends Serializable {

  /** Evaluates the trust scores of contributors in a distributed way.
    *
    * @param revisions
    *   RDD of revisions
    * @param contributorTemplatePositions
    *   Sequence of template positions related to contributor trustworthiness
    * @return
    *   RDD of contributor IDs and their trust scores
    */
  def evaluateContributorsDistributed(
      revisionsGraph: Graph[RevisionVertex, Byte],
      contributorTemplatePositions: Seq[Int]
  ): RDD[(Int, Float)] = {
    // Get all ground truth vertices and their scores
    val groundTruthScores = revisionsGraph.vertices
      .filter { case (_, vertex) => vertex.isGroundTruth }
      .flatMap { case (revId, vertex) =>
        // For each template position, calculate positive/negative scores
        contributorTemplatePositions.flatMap { pos =>
          val score = if (vertex.templateAdded.get(pos)) {
            -1.0f // Negative score for adding templates
          } else if (vertex.templateRemoved.get(pos)) {
            1.0f // Positive score for removing templates
          } else {
            0.0f
          }
          
          if (Math.abs(score) >= 0.0001f) {
            // Find edges to parent revisions (isChildOf)
            val parentEdges = revisionsGraph.edges
              .filter(e => e.srcId == revId && e.attr == EdgeType.isChildOf)
              .collect()
            
            // For each parent edge, get the parent's contributor
            parentEdges.flatMap { edge =>
              val parentVertex = revisionsGraph.vertices
                .filter(_._1 == edge.dstId)
                .map(_._2)
                .first()
              
              if (parentVertex.contributorId != -1) {
                Some((parentVertex.contributorId, score))
              } else {
                None
              }
            }
          } else {
            Seq.empty
          }
        }
      }

    // Sum up scores for each contributor
    groundTruthScores
      .reduceByKey(_ + _)
      .mapValues(sum => math.max(-1.0f, math.min(1.0f, sum))) // Clamp between -1 and 1
  }
}
