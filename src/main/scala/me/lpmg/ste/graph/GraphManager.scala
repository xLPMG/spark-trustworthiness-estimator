package me.lpmg.ste.graph

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import java.nio.file.Path
import me.lpmg.ste.types.Types.bitSetToString
import me.lpmg.ste.types.Types.stringToBitSet
import me.lpmg.ste.types.Types.TemplateBitPositions
import org.apache.spark.graphx.Edge
import spire.std.string

object GraphManager {

  /** Saves the revision graph to the data folder.
    *
    * @param graphName
    *   The name of the graph
    * @param revisionGraph
    *   The revision graph to save
    */
  def saveGraph(
      spark: SparkSession,
      dataFolderPath: String,
      graphName: String,
      revisionGraph: Graph[RevisionVertex, Byte]
  ): Unit = {
    import spark.implicits._
    val graphFolderPath = Path.of(dataFolderPath).resolve(graphName)
    if (!graphFolderPath.toFile.exists()) {
      graphFolderPath.toFile.mkdirs()
    }

    // Convert vertices to DataFrame with flattened fields
    val verticesDF = revisionGraph.vertices
      .map { case (id: Long, rev) =>
        // Convert BitSets to byte arrays for storage
        val templatePresenceString =
          bitSetToString(rev.templatePresence)
        val templateAddedString = bitSetToString(rev.templateAdded)
        val templateRemovedString = bitSetToString(rev.templateRemoved)

        (
          id: Long,
          rev.trustScore,
          rev.contributorId,
          templatePresenceString,
          templateAddedString,
          templateRemovedString
        )
      }
      .toDF(
        "id",
        "trustScore",
        "contributorId",
        "templatePresence",
        "templateAdded",
        "templateRemoved"
      )

    // Convert edges to DataFrame
    val edgesDF = revisionGraph.edges
      .map { edge =>
        (edge.srcId: Long, edge.dstId: Long, edge.attr)
      }
      .toDF("src", "dst", "attr")

    // Save vertices with partitioning and compression
    verticesDF.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(graphFolderPath.resolve("vertices_parquet").toString)

    // Save edges with partitioning and compression
    edgesDF.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(graphFolderPath.resolve("edges_parquet").toString)
  }

  /** Loads a saved revision graph from the data folder.
    *
    * @param graphName
    *   The name of the graph to load
    * @return
    *   The loaded Graph[RevisionVertex, Byte]
    */
  def loadGraph(
      spark: SparkSession,
      dataFolderPath: String,
      graphName: String
  ): Graph[RevisionVertex, Byte] = {
    import spark.implicits._
    val graphFolderPath = Path.of(dataFolderPath).resolve(graphName)

    // Load vertices DataFrame
    val verticesDF = spark.read
      .parquet(graphFolderPath.resolve("vertices_parquet").toString)

    // Convert back to RDD[(VertexId, RevisionVertex)]
    val vertices = verticesDF.rdd.map { row =>
      val id =
        row
          .getAs[Number]("id")
          .longValue() // Handle both Int and Long numerically
      val trustScore = row.getAs[Float]("trustScore")
      val contributorId = row.getAs[Int]("contributorId")

      // Convert byte arrays back to BitSets
      val templatePresenceString = row.getAs[String]("templatePresence")
      val templateAddedString = row.getAs[String]("templateAdded")
      val templateRemovedString = row.getAs[String]("templateRemoved")

      val templatePresence = stringToBitSet(
        templatePresenceString,
        TemplateBitPositions.size
      )
      val templateAdded =
        stringToBitSet(templateAddedString, TemplateBitPositions.size)
      val templateRemoved =
        stringToBitSet(templateRemovedString, TemplateBitPositions.size)

      (
        id,
        new RevisionVertex(
          id,
          trustScore,
          contributorId,
          templatePresence,
          templateAdded,
          templateRemoved
        )
      )
    }

    // Load edges DataFrame
    val edgesDF = spark.read
      .parquet(graphFolderPath.resolve("edges_parquet").toString)

    // Convert back to RDD[Edge[Byte]]
    val edges = edgesDF.rdd.map { row =>
      Edge(
        row
          .getAs[Number]("src")
          .longValue(),
        row
          .getAs[Number]("dst")
          .longValue(),
        row.getAs[Byte]("attr")
      )
    }

    // Create and return the graph
    Graph(vertices, edges)
  }
}
