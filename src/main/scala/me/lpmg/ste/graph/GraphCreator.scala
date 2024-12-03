package me.lpmg.ste.graph

import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.Revision
import org.apache.spark.broadcast.Broadcast
import me.lpmg.ste.types.EdgeType
import me.lpmg.ste.types.RevisionVertex

/** Provides functionality to create a revision graph using GraphX.
  */
object GraphCreator {

  /**
    * Create a revision graph from a sequence of revisions.
    *
    * @param revisionsRDD RDD of revisions
    * @return GraphX graph
    */
  def createRevisionGraph(
      revisionsRDD: RDD[Revision]
  ): Graph[RevisionVertex, Byte] = {
    // Create vertex for each revision. using revisionId as VertexId
    val vertices: RDD[(VertexId, RevisionVertex)] = revisionsRDD.map { rev =>
      (rev.revisionId, rev.toRevisionVertex)
    }

    // Connect revisions of the same page
    val temporalEdges: RDD[Edge[Byte]] = revisionsRDD.flatMap { rev =>
      if (rev.parentId != -1L) {
        Seq(
          Edge(rev.parentId, rev.revisionId, EdgeType.isParentOf),
          Edge(rev.revisionId, rev.parentId, EdgeType.isChildOf)
        )
      } else {
        Seq.empty
      }
    }

    // remove edges for which at least one of the vertices is not present
    val validVertexIds = vertices.map(_._1).collect().toSet
    val filteredTemporalEdges = temporalEdges.filter { edge =>
      validVertexIds.contains(edge.srcId) && validVertexIds.contains(edge.dstId)
    }

    // Create the GraphX graph
    Graph(vertices, filteredTemporalEdges)
  }
}
