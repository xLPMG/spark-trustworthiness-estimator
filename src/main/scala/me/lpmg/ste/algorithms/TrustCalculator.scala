package me.lpmg.ste.algorithms

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeDirection
import me.lpmg.ste.graph.RevisionVertex
import me.lpmg.ste.types.Types

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
      val templateAddedScore =
        (vertex.templateAdded
          .cardinality() * -initialGroundTruthScore) / Types.TemplateBitPositions.size
      val templateRemovedScore =
        (vertex.templateRemoved
          .cardinality() * initialGroundTruthScore) / Types.TemplateBitPositions.size

      val trustScore = templateAddedScore + templateRemovedScore
      vertex.copy(trustScore = trustScore)
    }
  }
}
