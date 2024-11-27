package me.lpmg.ste.algorithms

import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.Revision
import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeDirection
import me.lpmg.ste.types.EdgeType

object TrustCalculator {

  def initializeTrustScores(
      graph: Graph[RevisionVertex, Byte]
  ): Graph[RevisionVertex, Byte] = {
    graph.mapVertices { case (id, vertex) =>
    //   if (vertex.templateBitset.get(0)) {
    //     vertex.copy(isGroundTruth = true, trustScore = 1.0f)
    //   } else {
    //     vertex
    //   }
    vertex
    }
  }

  def computeTrustRank(
      graph: Graph[RevisionVertex, Byte],
      spark: SparkSession
  ): Graph[RevisionVertex, Byte] = {
    implicit val sc = spark.sparkContext
    // Define damping factor and number of iterations
    val dampingFactor = 0.85f

    // Compute the out-degree of each vertex
    val outDegreesBroadcast = sc.broadcast(graph.outDegrees.collectAsMap())

    // Compute the in-degree of each vertex
    val inDegrees = graph.inDegrees.cache()

    // Compute the number of vertices
    val numVertices = graph.numVertices

    // Initialize the trust scores
    var trustScores = graph.vertices.mapValues(vertex => vertex.trustScore)

    //////////////////////////////////////////////////////////////
    // TEMPORAL DECAY
    //////////////////////////////////////////////////////////////
    // val groundTruthVertices = graph.vertices.filter { case (_, v) =>
    //   v.isGroundTruth
    // }
    // val groundTruthIds = groundTruthVertices.map(_._1).collect().toSet
    val parent = propagateTrustScores(graph, EdgeType.isParentOf, 0.1f)
    val child = propagateTrustScores(graph, EdgeType.isChildOf, 0.1f)

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
    val initialMessage = 0.0 // Initial message: no score propagated yet

    val propagatedGraph = graph.pregel(initialMessage)(
      // Vertex Program: Update the vertex trustScore based on incoming message
      (id, vertex, incomingScore) => {
        if (vertex.trustScore != 0.0)
          vertex // Ground truth or already set; retain current vertex
        else
          vertex.copy(trustScore =
            incomingScore.toFloat
          ) // Accept propagated score
      },

      // Send Message: Propagate trustScore to neighbors
      triplet => {
        if (
          triplet.attr == edgeType && triplet.srcAttr.trustScore != 0.0 && math
            .abs(
              triplet.srcAttr.trustScore
            ) > decrement && triplet.dstAttr.trustScore == 0.0
        ) {
          // Decrease the score by decrement, respecting the sign
          val propagatedScore = triplet.srcAttr.trustScore - math.signum(
            triplet.srcAttr.trustScore
          ) * decrement
          Iterator((triplet.dstId, propagatedScore))
        } else {
          Iterator.empty // Stop propagation for non-'A' edges, low scores, or already scored nodes
        }
      },

      // Message Combiner: Combine messages (use the highest absolute value score)
      (msg1, msg2) => if (math.abs(msg1) > math.abs(msg2)) msg1 else msg2
    )
    propagatedGraph
  }
}
