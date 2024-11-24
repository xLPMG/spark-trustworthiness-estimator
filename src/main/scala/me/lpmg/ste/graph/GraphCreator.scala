package me.lpmg.ste.graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision

/** Provides functionality to create a revision graph using GraphX.
  */
object GraphCreator {
  def createRevisionGraph(
      spark: SparkSession,
      revisionsRDD: RDD[Revision]
  ): Graph[Revision, Byte] = {
    // Create vertex for each revision. using revisionId as VertexId
    val vertices: RDD[(VertexId, Revision)] = revisionsRDD.map { rev =>
      (rev.revisionId, rev)
    }

    // Connect revisions of the same page
    val temporalEdges: RDD[Edge[Byte]] = revisionsRDD.flatMap { rev =>
      rev.parentId
        .map { parentId =>
          Seq(
            Edge(parentId, rev.revisionId, EdgeType.isParentOf),
            Edge(rev.revisionId, parentId, EdgeType.isChildOf)
          )
        }
        .getOrElse(Seq.empty)
    }

    // Connect revisions with outlinks
    val outlinkEdges: RDD[Edge[Byte]] = revisionsRDD.flatMap { rev =>
      rev.resolvedRevisionOutlinks.flatMap { outlinkId =>
        Seq(
          Edge(rev.revisionId, outlinkId, EdgeType.linksTo),
          Edge(outlinkId, rev.revisionId, EdgeType.linkedFrom)
        )
      }
    }

    val allEdges = temporalEdges.union(outlinkEdges)

    // Create the GraphX graph
    Graph(vertices, allEdges)
  }

  final object EdgeType {
    final val isParentOf: Byte = 0.toByte
    final val isChildOf: Byte = 1.toByte
    final val linksTo: Byte = 2.toByte
    final val linkedFrom: Byte = 3.toByte
  }
}
