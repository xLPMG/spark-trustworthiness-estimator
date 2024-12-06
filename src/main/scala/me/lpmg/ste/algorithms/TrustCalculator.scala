package me.lpmg.ste.algorithms

import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.Revision
import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeDirection
import me.lpmg.ste.types.EdgeType

final case class TrustMessage(score: Float, steps: Int)

object TrustCalculator extends Serializable {

  /** Initialize the trust scores of the vertices in the graph.
    *
    * @param graph
    * @return
    */
  def initializeTrustScores(
      graph: Graph[RevisionVertex, Byte],
      initialGroundTruthScore: Float = 1.0f
  ): Graph[RevisionVertex, Byte] = {
    graph.mapVertices { case (id, vertex) =>
      if (vertex.templateAdded.cardinality() > 0) {
        vertex.copy(trustScore = -initialGroundTruthScore)
      } else if (vertex.templateRemoved.cardinality() > 0) {
        vertex.copy(trustScore = initialGroundTruthScore)
      } else {
        vertex.copy(trustScore = 0.0f)
      }
    }
  }

  /** Compute the trust scores of the vertices in the graph.
    *
    * @param graph
    * @param spark
    * @return
    */
  def computeTrustScores(
      graph: Graph[RevisionVertex, Byte],
      spark: SparkSession
  ): Graph[RevisionVertex, Byte] = {
    implicit val sc = spark.sparkContext
    //////////////////////////////////////////////////////////////
    // TEMPORAL DECAY
    //////////////////////////////////////////////////////////////
    val parent = propagateTemporalTrustScores(graph, EdgeType.isParentOf, 0.1f)
    val child = propagateTemporalTrustScores(graph, EdgeType.isChildOf, 0.2f)

    // Combine the trust scores from parent and child graphs
    val combinedTrustScores = parent.vertices.innerJoin(child.vertices) {
      case (id, parentVertex, childVertex) =>
        val combinedScore = parentVertex.trustScore + childVertex.trustScore
        val clampedScore = math.max(-1.0f, math.min(1.0f, combinedScore))
        parentVertex.copy(trustScore = clampedScore)
    }

    // Create a new graph with the combined trust scores
    Graph(combinedTrustScores, graph.edges)
  }

  /** Propagate trust scores along temporal edges
    *
    * @param graph
    *   the graph
    * @param edgeType
    *   type of edge along which trust is propagated
    * @param decrement
    *   amount by which the trust score is decremented each step
    * @return
    *   the graph with updated trust scores
    */
  private def propagateTemporalTrustScores(
      graph: Graph[RevisionVertex, Byte],
      edgeType: Byte,
      decrement: Float
  ): Graph[RevisionVertex, Byte] = {
    // for tracking if the vertex has been visited
    final case class VertexState(vertex: RevisionVertex, visited: Boolean)
    val initialMsg = TrustMessage(0.0f, 0)
    def isGroundTruth(vertex: RevisionVertex): Boolean = {
      vertex.templateAdded.cardinality() > 0 || vertex.templateRemoved
        .cardinality() > 0
    }

    // Ground Truth nodes
    val initialGraph = graph.mapVertices { case (id, vertex) =>
      VertexState(vertex, isGroundTruth(vertex))
    }
    val propagatedGraph = initialGraph.pregel(initialMsg)(
      (id, state, msg) => {
        if (msg == initialMsg) {
          state
        } else {
          val currentScore = state.vertex.trustScore
          if (isGroundTruth(state.vertex)) {
            state
          } else if (math.abs(msg.score) > math.abs(currentScore)) {
            VertexState(state.vertex.copy(trustScore = msg.score), true)
          } else {
            state.copy(visited = false)
          }
        }
      },
      // Send Message
      triplet => {
        if (triplet.attr != edgeType || !triplet.srcAttr.visited) {
          Iterator.empty
        } else {
          val srcVertex = triplet.srcAttr.vertex
          val dstVertex = triplet.dstAttr.vertex
          val srcScore = srcVertex.trustScore
          val dstScore = dstVertex.trustScore

          // the stopping conditions are if the score is less than the decrement
          // or if the destination vertex is a ground truth
          if (math.abs(srcScore) <= decrement) {
            Iterator.empty
          } else if (isGroundTruth(dstVertex)) {
            Iterator.empty
          } else {
            val newAbsScore = math.abs(srcScore) - decrement
            if (newAbsScore < decrement) {
              Iterator.empty
            } else {
              val newScore = if (srcScore > 0) newAbsScore else -newAbsScore
              if (math.abs(newScore) <= math.abs(dstScore)) {
                Iterator.empty
              } else {
                Iterator((triplet.dstId, TrustMessage(newScore, 0)))
              }
            }
          }
        }
      },
      (msg1, msg2) => {
        if (math.abs(msg1.score) > math.abs(msg2.score)) msg1 else msg2
      }
    )

    // return only the vertex data
    propagatedGraph.mapVertices { case (id, state) => state.vertex }
  }
}