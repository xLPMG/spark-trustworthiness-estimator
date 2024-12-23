package me.lpmg.ste.graph

import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.Revision

/** Provides functionality to create a revision graph using GraphX.
  */
object GraphCreator {

  /** Create a revision graph from a sequence of revisions.
    *
    * @param revisionsRDD
    *   RDD of revisions
    * @return
    *   GraphX graph
    */
  def createRevisionGraph(
      revisionsRDD: RDD[Revision]
  ): Graph[VertexType, Byte] = {
    // Create vertex for each revision. using revisionId as VertexId
    var vertices: RDD[(VertexId, VertexType)] = revisionsRDD.map { rev =>
      (rev.revisionId, rev.toRevisionVertex)
    }

    val sourceVertices: RDD[(VertexId, VertexType)] = revisionsRDD
      .flatMap(_.sources)
      .distinct()
      .map { source =>
        val sourceId = -source.hashCode.toLong
        val sourceVertex = new SourceVertex(sourceId, source, 0.0f)
        (
          sourceId,
          sourceVertex
        )
      }

    vertices = vertices ++ sourceVertices

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

    // Connect revisions to their sources
    val sourceEdges: RDD[Edge[Byte]] = revisionsRDD.flatMap { rev =>
      rev.sources.flatMap { source =>
        val sourceId = source.hashCode.toLong
        Seq(
          Edge(-sourceId, rev.revisionId, EdgeType.hasSource),
          Edge(rev.revisionId, -sourceId, EdgeType.isReferencedBy)
        )
      }
    }

    val allEdges = temporalEdges ++ sourceEdges

    // remove edges for which at least one of the vertices is not present
    // val validVertexIds = vertices.map(_._1).collect().toSet
    // val filteredEdges = allEdges.filter { edge =>
    //   validVertexIds.contains(edge.srcId) && validVertexIds.contains(edge.dstId)
    // }

    // Create the GraphX graph
    Graph(vertices, allEdges)
  }
}
