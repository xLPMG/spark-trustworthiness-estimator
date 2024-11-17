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
            Edge(parentId.toLong, rev.revisionId.toLong, "isParentOf"),
            Edge(rev.revisionId.toLong, parentId.toLong, "isChildOf")
          )
        }
        .getOrElse(Seq.empty)
    }

    // Connect revisions with outlinks
    val outlinkEdges: RDD[Edge[String]] = revisionsRDD.flatMap { rev =>
      rev.outlinks.flatMap { outlinkId =>
        Seq(
          Edge(rev.revisionId.toLong, outlinkId.toLong, "linksTo"),
          Edge(outlinkId.toLong, rev.revisionId.toLong, "linkedFrom")
        )
      }
    }

    val allEdges = temporalEdges.union(outlinkEdges)

    // Create the GraphX graph
    Graph(vertices, allEdges)
  }
}
