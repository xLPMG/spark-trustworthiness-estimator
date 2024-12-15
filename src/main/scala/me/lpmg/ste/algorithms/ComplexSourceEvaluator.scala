package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import me.lpmg.ste.graph.VertexType
import org.apache.spark.graphx.{Graph, Edge, VertexId}
import me.lpmg.ste.graph.RevisionVertex

object ComplexSourceEvaluator {
  
    def initializeVertices(
        vertices: RDD[(VertexId, VertexType)],
        sourceTemplatePositions: Seq[Int]
    ): RDD[(VertexId, Float)] = {
        vertices.map {
            case (id, RevisionVertex(_, _, _, _, templateAdded, templateRemoved, _)) =>
                val templateAddedCount = sourceTemplatePositions.count(pos => templateAdded.get(pos))
                val templateRemovedCount = sourceTemplatePositions.count(pos => templateRemoved.get(pos))
                val templateImpact = (-templateAddedCount + templateRemovedCount).toFloat
                (id, templateImpact)
        }
    }

}
