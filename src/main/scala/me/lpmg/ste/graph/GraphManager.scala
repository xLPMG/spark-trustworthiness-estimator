package me.lpmg.ste.graph

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.types.Types.{
  BitSetToByteArray,
  ByteArrayToBitSet,
  TemplateBitPositions
}
import me.lpmg.ste.time.Watch
import java.nio.file.Path
import me.lpmg.ste.data.DataReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.Revision
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZonedDateTime
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.util.collection.BitSet
import me.lpmg.ste.data.TemplateUpdater
import org.apache.spark.graphx.Edge
import me.lpmg.ste.types.EdgeType

class GraphManager(
    spark: SparkSession,
    dumpFolderPath: String,
    dataFolderPath: String
) {
  import spark.implicits._
  val logger = Logger(getClass.getName)
  var dateLimit: Long = 0

  /** Sets the date limit for the graph. Only revisions after this date will be
    * included in the graph.
    *
    * @param date
    */
  def setDateLimit(date: ZonedDateTime): Unit = {
    setDateLimit(date.toInstant().toEpochMilli())
  }

  /** Sets the date limit for the graph. Only revisions after this date will be
    * included in the graph.
    *
    * @param date
    */
  def setDateLimit(date: Long): Unit = {
    dateLimit = date
  }

  /** Initializes the graph by reading all revisions from the dump folder.
    *
    * @return
    *   The revision graph
    */
  def initializeGraph(): Graph[RevisionVertex, Byte] = {
    // Read all .xml.bz2 files in the folder into an RDD
    // val filesRDD = spark.sparkContext.binaryFiles(s"$dumpFolderPath/*.bz2")

    val filesRDD = spark.sparkContext
      .binaryFiles(s"$dumpFolderPath/*.bz2")
      .zipWithIndex()
      .filter(_._2 < 3)
      .map(_._1)

    logger.warn(s"Total files found: ${filesRDD.count()}")

    /////////////////////////////////////////////////////////////////////////////////////////
    // REVISION EXTRACTION
    /////////////////////////////////////////////////////////////////////////////////////////
    val fixedDateLimit = dateLimit
    val allRevisionsRDD = filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionsFromPDS(
          pds,
          fixedDateLimit
        )
      }
      .cache()

    logger.warn(s"Total Revisions Extracted: ${allRevisionsRDD.count()}")

    // Group revisions by their page ID, sort by timestamp and save as MinimalRevision
    val groupedRevisionsRDD =
      allRevisionsRDD
        .groupBy(_.pageId)
        .mapValues { revisions =>
          // [oldest, ..., newest]
          revisions.toSeq.sortBy(_.timestamp).map { rev =>
            rev.toIdTimestampPair
          }
        }

    // Find oldest revisions and update parent IDs in a distributed way
    val updatedRevisionsRDD = if (fixedDateLimit <= 0) {
      allRevisionsRDD
    } else {
      logger.warn("Setting parent ID of oldest revisions to -1")
      
      // Create an RDD of oldest revision IDs with a marker
      val oldestRevisionsRDD = groupedRevisionsRDD
        .flatMap { case (pageId, revisions) => 
          revisions.headOption.map(rev => (rev._1, true))
        }
        .persist()  // Cache since we'll use this RDD twice
      
      // Use leftOuterJoin to efficiently mark oldest revisions
      allRevisionsRDD
        .keyBy(_.revisionId)  // Create key-value pairs for join
        .leftOuterJoin(oldestRevisionsRDD)
        .map { case (revId, (revision, isOldest)) =>
          if (isOldest.isDefined) {
            revision.copy(parentId = -1)
          } else {
            revision
          }
        }
        .persist()  // Cache the result as it will be used for template tracking
    }
    
    // Clean up cached RDD
    allRevisionsRDD.unpersist()

    /////////////////////////////////////////////////////////////////////////////////////////
    // TEMPLATE TRACKING
    /////////////////////////////////////////////////////////////////////////////////////////
    val revisionsWithTemplateBitSets = TemplateUpdater.updateTemplateBitSetsDistributed(updatedRevisionsRDD)

    /////////////////////////////////////////////////////////////////////////////////////////
    // GRAPH CREATION
    /////////////////////////////////////////////////////////////////////////////////////////
    logger.warn("Creating revision graph")
    val revisionGraph =
      GraphCreator.createRevisionGraph(revisionsWithTemplateBitSets)
    revisionGraph
  }

  /** Saves the revision graph to the data folder.
    *
    * @param graphName
    *   The name of the graph
    * @param revisionGraph
    *   The revision graph to save
    */
  def saveGraph(
      graphName: String,
      revisionGraph: Graph[RevisionVertex, Byte]
  ): Unit = {
    val graphFolderPath = Path.of(dataFolderPath).resolve(graphName)
    if (!graphFolderPath.toFile.exists()) {
      graphFolderPath.toFile.mkdirs()
    }

    // Convert vertices to DataFrame with flattened fields
    val verticesDF = revisionGraph.vertices
      .map { case (id: Long, rev) =>
        // Convert BitSets to byte arrays for storage
        val templatePresenceBytes =
          BitSetToByteArray(rev.templatePresence)
        val templateAddedBytes = BitSetToByteArray(rev.templateAdded)
        val templateRemovedBytes = BitSetToByteArray(rev.templateRemoved)

        (
          id: Long,
          rev.trustScore,
          rev.contributorId,
          templatePresenceBytes,
          templateAddedBytes,
          templateRemovedBytes
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

    logger.warn("Graph saved successfully.")
  }

  /** Loads a saved revision graph from the data folder.
    *
    * @param graphName
    *   The name of the graph to load
    * @return
    *   The loaded Graph[RevisionVertex, Byte]
    */
  def loadGraph(graphName: String): Graph[RevisionVertex, Byte] = {
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
      val templatePresenceBytes = row.getAs[Array[Byte]]("templatePresence")
      val templateAddedBytes = row.getAs[Array[Byte]]("templateAdded")
      val templateRemovedBytes = row.getAs[Array[Byte]]("templateRemoved")

      val templatePresence = ByteArrayToBitSet(
        templatePresenceBytes,
        TemplateBitPositions.size
      )
      val templateAdded =
        ByteArrayToBitSet(templateAddedBytes, TemplateBitPositions.size)
      val templateRemoved =
        ByteArrayToBitSet(templateRemovedBytes, TemplateBitPositions.size)

      (
        id,
        new RevisionVertex(
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
          .longValue(), // Handle both Int and Long numerically
        row
          .getAs[Number]("dst")
          .longValue(), // Handle both Int and Long numerically
        row.getAs[Byte]("attr")
      )
    }

    // Create and return the graph
    Graph(vertices, edges)
  }

}
