package me.lpmg.ste.algorithms

import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.EdgeType

object ContributorEvaluator extends Serializable {

  /** Evaluates the trust scores of contributors in the graph.
    *
    * @param graph
    *   Graph of revisions
    * @return
    *   Map of contributor IDs and their trust scores
    */
  def evaluateContributors(
      graph: Graph[RevisionVertex, Byte]
  ): Map[Int, Float] = {
    evaluateContributorsDistributed(graph).collect().toMap
  }

  /** Evaluates the trust scores of contributors in the graph in a distributed
    * way.
    *
    * @param graph
    *   Graph of revisions
    * @return
    *   RDD of contributor IDs and their trust scores
    */
  def evaluateContributorsDistributed(
      graph: Graph[RevisionVertex, Byte]
  ): RDD[(Int, Float)] = {
    // get parent trust scores using edges where attr = EdgeType.isParentOf
    val parentScores = graph.triplets
      .filter(_.attr == EdgeType.isParentOf)
      .map(triplet => (triplet.dstId, triplet.srcAttr.trustScore))
      .cache()

    // calculate impact based on templates added/removed and trust score changes
    val changes = graph.vertices
      .leftOuterJoin(parentScores)
      .map { case (_, (vertex, parentScoreOpt)) =>
        val trustScoreChange = parentScoreOpt match {
          case Some(parentScore) => vertex.trustScore - parentScore
          case None => vertex.trustScore // For root vertices with no parent
        }

        // calculate template impact
        val templateAddedCount = vertex.templateAdded.cardinality()
        val templateRemovedCount = vertex.templateRemoved.cardinality()
        val templateImpact =
          if (templateAddedCount > 0 || templateRemovedCount > 0) {
            // negative impact for adding templates, positive for removing
            (-templateAddedCount + templateRemovedCount).toFloat
          } else 0.0f

        // combine trust score change with template impact
        val totalImpact =
          if (templateImpact != 0) {
            // weight template changes more heavily for ground truth nodes
            trustScoreChange * 0.25f + templateImpact * 0.75f
          } else {
            trustScoreChange
          }

        (vertex.contributorId, totalImpact)
      }
      .cache()

    // find maximum absolute value for normalization
    val maxAbsChange = changes
      .map { case (_, score) => math.abs(score) }
      .max()

    // normalize scores
    val normalizedChanges = changes
      .mapValues(score =>
        if (maxAbsChange > 0) score / maxAbsChange
        else score
      )
      .reduceByKey(_ + _)
      .mapValues(sum => math.max(-1.0f, math.min(1.0f, sum)))

    normalizedChanges
  }

  /** Applies the trust scores of contributors to the graph.
    *
    * @param graph
    *   Graph of revisions
    * @param contributorScores
    *   RDD of contributor IDs and their trust scores
    * @param contributorScoreImportance
    *   Importance of contributor trust scores
    * @return
    *   Graph with contributor trust scores applied
    */
  def applyContributorTrustScores(
      graph: Graph[RevisionVertex, Byte],
      contributorScores: RDD[(Int, Float)],
      contributorScoreImportance: Float
  ): Graph[RevisionVertex, Byte] = {
    // Create vertices RDD with contributor IDs
    val verticesWithContributors = graph.vertices.map { case (id, vertex) =>
      (vertex.contributorId, (id, vertex))
    }

    // Join with contributor scores
    val joinedScores = verticesWithContributors
      .leftOuterJoin(contributorScores)
      .map { case (contributorId, ((vertexId, vertex), contributorScoreOpt)) =>
        (vertexId, (vertex, contributorScoreOpt.getOrElse(0.0f)))
      }

    // Create new graph with updated scores
    Graph(
      joinedScores.map { case (vertexId, (vertex, contributorScore)) =>
        (
          vertexId,
          vertex.copy(
            trustScore =
              vertex.trustScore * (1 - contributorScoreImportance) + contributorScore * contributorScoreImportance
          )
        )
      },
      graph.edges
    )
  }

  /** Applies the trust scores of contributors to the graph without applying
    * trust scores to ground truth nodes
    *
    * @param graph
    *   Graph of revisions
    * @param contributorScores
    *   RDD of contributor IDs and their trust scores
    * @param contributorScoreImportance
    *   Importance of contributor trust scores
    * @return
    *   Graph with contributor trust scores applied
    */
  def applyContributorTrustScoresWithoutGroundTruths(
      graph: Graph[RevisionVertex, Byte],
      contributorScores: RDD[(Int, Float)],
      contributorScoreImportance: Float
  ): Graph[RevisionVertex, Byte] = {
    // Create vertices RDD with contributor IDs
    val verticesWithContributors = graph.vertices.map { case (id, vertex) =>
      (vertex.contributorId, (id, vertex))
    }

    // Join with contributor scores
    val joinedScores = verticesWithContributors
      .leftOuterJoin(contributorScores)
      .map { case (contributorId, ((vertexId, vertex), contributorScoreOpt)) =>
        (vertexId, (vertex, contributorScoreOpt.getOrElse(0.0f)))
      }

    // Create new graph with updated scores
    Graph(
      joinedScores.map { case (vertexId, (vertex, contributorScore)) =>
        if (
          vertex.templateAdded.cardinality() > 0 || vertex.templateRemoved
            .cardinality() > 0
        ) {
          // Don't modify ground truth vertices
          (vertexId, vertex)
        } else {
          (
            vertexId,
            vertex.copy(
              trustScore =
                vertex.trustScore * (1 - contributorScoreImportance) + contributorScore * contributorScoreImportance
            )
          )
        }
      },
      graph.edges
    )
  }

}
