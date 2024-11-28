package me.lpmg.ste.algorithms

import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.Revision
import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeDirection
import me.lpmg.ste.types.EdgeType

case class TrustMessage(score: Float, steps: Int)

object TrustCalculator extends Serializable {

  def initializeTrustScores(
      graph: Graph[RevisionVertex, Byte]
  ): Graph[RevisionVertex, Byte] = {
    graph.mapVertices { case (id, vertex) =>
      if (vertex.templateAdded.cardinality() > 0) {
        vertex.copy(trustScore = -1.0f)
      } else if (vertex.templateRemoved.cardinality() > 0) {
        vertex.copy(trustScore = 1.0f)
      } else {
        vertex.copy(trustScore = 0.0f)
      }
    }
  }

  def computeTrustRank(
      graph: Graph[RevisionVertex, Byte],
      spark: SparkSession
  ): Graph[RevisionVertex, Byte] = {
    implicit val sc = spark.sparkContext
    //////////////////////////////////////////////////////////////
    // TEMPORAL DECAY
    //////////////////////////////////////////////////////////////
    val parent = propagateTrustScores(graph, EdgeType.isParentOf, 0.1f)
    val child = propagateTrustScores(graph, EdgeType.isChildOf, 0.2f)

    // Combine the trust scores from parent and child graphs
    val combinedTrustScores = parent.vertices.innerJoin(child.vertices) {
      case (id, parentVertex, childVertex) =>
        val combinedScore = parentVertex.trustScore + childVertex.trustScore
        val clampedScore = math.max(-1.0f, math.min(1.0f, combinedScore))
        parentVertex.copy(trustScore = clampedScore)
    }

    // Create a new graph with the combined trust scores
    val combinedGraph = Graph(combinedTrustScores, graph.edges)

    combinedGraph
  }

  private def propagateTrustScores(
      graph: Graph[RevisionVertex, Byte],
      edgeType: Byte,
      decrement: Float
  ): Graph[RevisionVertex, Byte] = {
    // We'll track both the score and whether this vertex has been updated
    case class VertexState(vertex: RevisionVertex, changed: Boolean)
    
    // Initialize vertices - identify starting nodes by their template properties
    val initialGraph = graph.mapVertices { case (id, vertex) =>
      val isStartingNode = vertex.templateAdded.cardinality() > 0 || 
                          vertex.templateRemoved.cardinality() > 0
      VertexState(vertex, isStartingNode)
    }

    val initialMsg = TrustMessage(0.0f, 0)
    
    val propagatedGraph = initialGraph.pregel(initialMsg)(
      // Vertex Program
      (id, state, msg) => {
        if (msg == initialMsg) {
          state
        } else {
          val currentScore = state.vertex.trustScore
          val isStartingNode = state.vertex.templateAdded.cardinality() > 0 || 
                              state.vertex.templateRemoved.cardinality() > 0
          if (isStartingNode) {
            state
          } else if (math.abs(msg.score) > math.abs(currentScore)) {
            VertexState(state.vertex.copy(trustScore = msg.score), true)
          } else {
            state.copy(changed = false)
          }
        }
      },
      // Send Message
      triplet => {
        if (triplet.attr != edgeType || !triplet.srcAttr.changed) {
          Iterator.empty
        } else {
          val srcVertex = triplet.srcAttr.vertex
          val dstVertex = triplet.dstAttr.vertex
          val srcScore = srcVertex.trustScore
          val dstScore = dstVertex.trustScore
          
          val isDstStartingNode = dstVertex.templateAdded.cardinality() > 0 || 
                                 dstVertex.templateRemoved.cardinality() > 0
          
          if (math.abs(srcScore) <= decrement) {
            Iterator.empty
          } else if (isDstStartingNode) {
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
      // Merge Message
      (msg1, msg2) => {
        if (math.abs(msg1.score) > math.abs(msg2.score)) msg1 else msg2
      }
    )

    // Return only the vertex data
    propagatedGraph.mapVertices { case (id, state) => state.vertex }
  }
}
