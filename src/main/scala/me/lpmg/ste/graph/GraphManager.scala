package me.lpmg.ste.graph

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.Types
import me.lpmg.ste.time.Watch
import java.nio.file.Path
import me.lpmg.ste.data.DataReader
import org.apache.spark.sql.DataFrame
import me.lpmg.ste.data.LinkResolver
import org.apache.spark.graphx.Graph
import me.lpmg.ste.data.Revision

class GraphManager(
    spark: SparkSession,
    dumpFolderPath: String,
    dataFolderPath: String
) {
  import spark.implicits._
  val logger = Logger(getClass.getName)

  /**
    * Initializes the graph by reading all revisions from the dump folder.
    *
    * @return The revision graph
    */
  def initializeGraph(): Graph[Revision, Byte] = {
    // Read all .xml.bz2 files in the folder into an RDD
    // val filesRDD = spark.sparkContext.binaryFiles(s"$dumpFolderPath/*.bz2")

    val filesRDD = spark.sparkContext
      .binaryFiles(s"$dumpFolderPath/*.bz2")
      .zipWithIndex()
      .filter(_._2 < 3)
      .map(_._1)

    logger.warn(s"Total files found: ${filesRDD.count()}")

    var dictionary: Types.DictType = Map.empty

    /////////////////////////////////////////////////////////////////////////////////////////
    /// DICTIONARY
    /////////////////////////////////////////////////////////////////////////////////////////
    Watch.start("dictionary")
    val dictionaryFile: Path =
      Path.of(dataFolderPath).resolve("dictionary2.parquet")
    // WRITE
    if (!dataFolderPath.isEmpty && !dictionaryFile.toFile.exists()) {
      logger.warn(s"Creating dictionary file at: $dictionaryFile")
      // value = (filePath: String, fileContent: PortableDataStream)
      dictionary = filesRDD.aggregate(Map.empty[String, (Int, String)])(
        (acc, value) => acc ++ DataReader.getDictionaryFromPDS(value._2),
        (acc1, acc2) => acc1 ++ acc2
      )

      // Convert dictionary to DataFrame
      val dictionaryDF: DataFrame =
        dictionary.toSeq
          .map { case (pageTitle, (pageID, redirectTo)) =>
            (pageTitle, pageID, redirectTo)
          }
          .toDF("PageTitle", "PageID", "RedirectsTo")

      // Write DataFrame to Parquet
      dictionaryDF.write.parquet(dictionaryFile.toString)
    } else if (!dataFolderPath.isEmpty && dictionaryFile.toFile.exists()) {
      // READ
      logger.warn(s"Reading dictionary file from: $dictionaryFile")

      // Read DataFrame from Parquet
      val dictionaryDF: DataFrame = spark.read.parquet(dictionaryFile.toString)

      // Convert DataFrame to Map
      dictionary = dictionaryDF
        .collect()
        .map(row => row.getString(0) -> (row.getInt(1), row.getString(2)))
        .toMap
    }
    val broadCastedDictionary = spark.sparkContext.broadcast(dictionary)
    logger.warn(
      s"Finished processing dictionary (${Watch.stopFormatted("dictionary")})"
    )
    /////////////////////////////////////////////////////////////////////////////////////////
    // REVISION EXTRACTION
    /////////////////////////////////////////////////////////////////////////////////////////
    val allRevisionsRDD = filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionsFromPDS(pds, broadCastedDictionary.value)
      }
      .cache()

    logger.warn(s"Total Revisions Extracted: ${allRevisionsRDD.count()}")

    // Group revisions by their page ID, sort by timestamp and save as MinimalRevision
    val groupedRevisionsRDD =
      allRevisionsRDD
        .groupBy(_.pageId)
        .mapValues { revisions =>
          revisions.toSeq.sortBy(_.timestamp).map { rev =>
            rev.toMinimalRevision
          }
        }
    val groupedRevisionsMap = groupedRevisionsRDD.collectAsMap().toMap

    /////////////////////////////////////////////////////////////////////////////////////////
    // LINK RESOLUTION
    /////////////////////////////////////////////////////////////////////////////////////////
    val resolvedRevisionsRDD = allRevisionsRDD.map { revision =>
      LinkResolver.resolvePageIDsToRevisionIDs(
        revision,
        groupedRevisionsMap
      )
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    // GRAPH CREATION
    /////////////////////////////////////////////////////////////////////////////////////////
    val revisionGraph = GraphCreator.createRevisionGraph(resolvedRevisionsRDD)
    allRevisionsRDD.unpersist()
    revisionGraph
  }

  /**
    * Saves the revision graph to the data folder.
    *
    * @param graphName The name of the graph
    */
  def saveGraph(graphName: String, revisionGraph: Graph[Revision, Byte]): Unit = {
    val graphFolderPath = Path.of(dataFolderPath).resolve(graphName)
    if (!graphFolderPath.toFile.exists()) {
      graphFolderPath.toFile.mkdirs()
    }

    // Convert vertices to DataFrame with flattened fields
    val verticesDF = revisionGraph.vertices
      .map { case (id, rev) =>
        (
          id,
          rev.pageId,
          rev.timestamp,
          rev.isGroundTruth,
          rev.trustScore,
          rev.isRedirect
        )
      }
      .toDF(
        "id",
        "pageId",
        "timestamp",
        "isGroundTruth",
        "trustScore",
        "isRedirect"
      )

    // Convert edges to DataFrame
    val edgesDF = revisionGraph.edges.toDF("src", "dst", "attr")

    // Save vertices with partitioning and compression
    verticesDF.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(graphFolderPath.resolve("vertices_parquet").toString)

    // Save edges with partitioning and compression
    edgesDF.write
      .mode("overwrite")
      .option("compression", "snappy")
      .partitionBy("src")
      .parquet(graphFolderPath.resolve("edges_parquet").toString)

    logger.warn("Graph saved successfully.")
  }
}
