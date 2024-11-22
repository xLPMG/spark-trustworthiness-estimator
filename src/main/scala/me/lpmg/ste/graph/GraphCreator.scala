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
  ): Graph[Revision, String] = {
    // Create vertex for each revision. using revisionId as VertexId
    val vertices: RDD[(VertexId, Revision)] = revisionsRDD.map { rev =>
      (rev.revisionId.toLong, rev)
    }

    // Connect revisions of the same page
    val temporalEdges: RDD[Edge[String]] = revisionsRDD.flatMap { rev =>
      rev.parentId
        .map { parentId =>
          Seq(
            Edge(parentId, rev.revisionId, "isParentOf"),
            Edge(rev.revisionId, parentId, "isChildOf")
          )
        }
        .getOrElse(Seq.empty)
    }

    // Connect revisions with outlinks
    val outlinkEdges: RDD[Edge[String]] = revisionsRDD.flatMap { rev =>
      rev.resolvedRevisionOutlinks.flatMap { outlinkId =>
        Seq(
          Edge(rev.revisionId, outlinkId, "linksTo"),
          Edge(outlinkId, rev.revisionId, "linkedFrom")
        )
      }
    }

    val allEdges = temporalEdges.union(outlinkEdges)

  // Remove edges with missing vertices
  // TODO: check if there is a faster way
    val validVertices = vertices.map(_._1).collect().toSet
    val filteredEdges = allEdges.filter { edge =>
      validVertices.contains(edge.srcId) && validVertices.contains(edge.dstId)
    }

    // Create the GraphX graph
    Graph(vertices, filteredEdges)
  }
}
