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

    // create temporal edges
    val edges: RDD[Edge[String]] = revisionsRDD.flatMap { rev =>
      rev.parentId.map { parentId =>
        Seq(
          Edge(parentId.toLong, rev.revisionId.toLong, "isParentOf"),
          Edge(rev.revisionId.toLong, parentId.toLong, "isChildOf")
        )
      }.getOrElse(Seq.empty)
    }

    // Create the GraphX graph
    Graph(vertices, edges)
  }
}
