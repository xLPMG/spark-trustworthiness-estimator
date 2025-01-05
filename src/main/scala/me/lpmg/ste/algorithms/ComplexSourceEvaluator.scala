package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import me.lpmg.ste.graph.VertexType
import org.apache.spark.graphx.{Graph, Edge, VertexId}
import me.lpmg.ste.graph.RevisionVertex
import me.lpmg.ste.graph.SourceVertex
import me.lpmg.ste.graph.EdgeType

object ComplexSourceEvaluator {

  def initializeVertices(
      vertices: RDD[(VertexId, VertexType)],
      templatePosition: Int
  ): RDD[(VertexId, VertexType)] = {
    vertices.filter(_._2 != null).map {
      case (
            id,
            rev @ RevisionVertex(_, _, _, templateAdded, templateRemoved)
          ) =>
        val newScore =
          if (templateRemoved.get(templatePosition)) 0.0f // No template
          // TODO: check if templatePresence is better
          else if (templateAdded.get(templatePosition))
            1.0f // Contains template
          else 0.5f // Neutral trust score for unknown revisions
        (id, rev.copy(trustScore = newScore))
      case (id, src: SourceVertex) =>
        (id, src.copy(trustScore = 0.5f)) // Neutral trust score for sources
    }
  }

  def runPregel(
      graph: Graph[VertexType, Byte],
      temporalPropagation: Boolean = false,
      mergeFunction: (Double, Double) => Double = math.max
  ): Graph[VertexType, Byte] = {
    graph.pregel(initialMsg = 0.5, maxIterations = 4)(
      // Vertex Program: Update trust score based on incoming trust
      vprog = (id, currentTrust, newTrust) =>
        currentTrust match {
          case rev: RevisionVertex =>
            rev.copy(trustScore =
              math.max(currentTrust.trustScore, newTrust.toFloat)
            )
          case src: SourceVertex =>
            src.copy(trustScore =
              math.max(currentTrust.trustScore, newTrust.toFloat)
            )
          case null => null
        },

      // Send Message: Propagate trust scores forward
      sendMsg = edgeTriplet => {
        edgeTriplet.attr match {
          case EdgeType.hasSource =>
            Iterator((edgeTriplet.dstId, edgeTriplet.srcAttr.trustScore * 0.8))
          case EdgeType.isReferencedBy =>
            Iterator((edgeTriplet.dstId, edgeTriplet.srcAttr.trustScore * 0.5))
          case EdgeType.isParentOf =>
            if (temporalPropagation)
              Iterator(
                (edgeTriplet.dstId, edgeTriplet.srcAttr.trustScore * 0.2)
              )
            else Iterator.empty
          case _ => Iterator.empty
        }
      },

      // Merge Messages: Combine trust values
      mergeMsg = mergeFunction
    )
  }

}
