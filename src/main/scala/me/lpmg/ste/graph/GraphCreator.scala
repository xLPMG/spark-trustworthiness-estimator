package me.lpmg.ste.graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Graph, Edge, VertexId}
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision

/**
  * Provides functionality to create a revision graph using GraphX.
  */
object GraphCreator {
  def createRevisionGraph(spark: SparkSession, revisionsRDD: RDD[Revision]): Graph[Revision, String] = {
    // Assign each Revision a unique VertexId using its revisionId
    val vertices: RDD[(VertexId, Revision)] = revisionsRDD.map { rev =>
      (rev.revisionId.toLong, rev)  // Assuming revisionId can be cast to Long for VertexId
    }

    // Define edges between revisions, for example, linking revisions with their parentId if present
    val edges: RDD[Edge[String]] = revisionsRDD.flatMap { rev =>
      rev.parentId.map { parentId =>
        Edge(parentId.toLong, rev.revisionId.toLong, "parent")
      }
    }

    // Create the GraphX graph
    Graph(vertices, edges)
  }
}