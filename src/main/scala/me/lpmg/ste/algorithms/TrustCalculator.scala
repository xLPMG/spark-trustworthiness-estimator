package me.lpmg.ste.algorithms

import org.apache.spark.graphx.Graph
import me.lpmg.ste.data.Revision
import org.apache.spark.rdd.RDD

object TrustCalculator {

  def initTrustScores(
      revisionGraph: Graph[Revision, Byte],
      positiveSeeds: Seq[Long],
      negativeSeeds: Seq[Long],
      positiveSeedValue: Float,
      negativeSeedValue: Float
  ): Graph[Revision, Byte] = {
    val updatedVertices: RDD[(Long, Revision)] =
      revisionGraph.vertices.map { case (id, rev) =>
        if (positiveSeeds.contains(id)) {
          (id, rev.copy(trustScore = positiveSeedValue))
        } else if (negativeSeeds.contains(id)) {
          (id, rev.copy(trustScore = negativeSeedValue))
        } else {
          (id, rev)
        }
      }
    Graph(updatedVertices, revisionGraph.edges)
  }

}
