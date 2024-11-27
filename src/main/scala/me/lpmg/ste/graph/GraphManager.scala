package me.lpmg.ste.graph

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.types.Types
import me.lpmg.ste.time.Watch
import java.nio.file.Path
import me.lpmg.ste.data.DataReader
import org.apache.spark.sql.DataFrame
import me.lpmg.ste.data.LinkResolver
import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.Revision
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZonedDateTime
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.util.collection.BitSet
import me.lpmg.ste.data.TemplateUpdater

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
    val fixedDateLimit = dateLimit
    val allRevisionsRDD = filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionsFromPDS(
          pds,
          broadCastedDictionary.value,
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

    // set parent ID of oldest revisions to -1
    // this is only needed in case we limit the graph by date
    val oldestRevisionIds =
      groupedRevisionsRDD
        .mapValues(_.headOption.map(_._1))
        .collect { case (pageId, Some(revisionId)) =>
          revisionId
        }
        .collect()
        .toSet
    val updatedRevisionsRDD = if (fixedDateLimit <= 0) {
      allRevisionsRDD
    } else {
      logger.warn("Setting parent ID of oldest revisions to -1")
      allRevisionsRDD.map { revision =>
        if (oldestRevisionIds.contains(revision.revisionId)) {
          revision.copy(parentId = -1)
        } else {
          revision
        }
      }
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    // TEMPLATE TRACKING
    /////////////////////////////////////////////////////////////////////////////////////////
    // Create a map of revision IDs to revisions for quick lookup
    val revisionMap =
      updatedRevisionsRDD.map(rev => rev.revisionId -> rev).collectAsMap().toMap

    // Update the templateAdded and templateRemoved BitSets for each revision
    val updatedTemplateRevisionsRDD = updatedRevisionsRDD.mapPartitions {
      partition =>
        val revisions = partition.toSeq
        TemplateUpdater.updateTemplateBitSets(revisions, revisionMap).iterator
    }

    allRevisionsRDD.unpersist()

    val groupedRevisionsMap = groupedRevisionsRDD.collectAsMap().toMap

    /////////////////////////////////////////////////////////////////////////////////////////
    // LINK RESOLUTION
    /////////////////////////////////////////////////////////////////////////////////////////
    val resolvedRevisionsRDD = updatedTemplateRevisionsRDD.map { revision =>
      LinkResolver.resolvePageIDsToRevisionIDs(
        revision,
        groupedRevisionsMap
      )
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    // GRAPH CREATION
    /////////////////////////////////////////////////////////////////////////////////////////
    logger.warn("Creating revision graph")
    val revisionGraph = GraphCreator.createRevisionGraph(resolvedRevisionsRDD)
    revisionGraph
  }

  // /** Saves the revision graph to the data folder.
  //   *
  //   * @param graphName
  //   *   The name of the graph
  //   * @param revisionGraph
  //   *   The revision graph to save
  //   */
  // def saveGraph(
  //     graphName: String,
  //     revisionGraph: Graph[Revision, Byte]
  // ): Unit = {
  //   val graphFolderPath = Path.of(dataFolderPath).resolve(graphName)
  //   if (!graphFolderPath.toFile.exists()) {
  //     graphFolderPath.toFile.mkdirs()
  //   }

  //   // Convert vertices to DataFrame with flattened fields
  //   val verticesDF = revisionGraph.vertices
  //     .map { case (id, rev) =>
  //       (
  //         id,
  //         rev.pageId,
  //         rev.timestamp,
  //         rev.isRedirect
  //       )
  //     }
  //     .toDF(
  //       "id",
  //       "pageId",
  //       "timestamp",
  //       "isRedirect"
  //     )

  //   // Convert edges to DataFrame
  //   val edgesDF = revisionGraph.edges.toDF("src", "dst", "attr")

  //   // Save vertices with partitioning and compression
  //   verticesDF.write
  //     .mode("overwrite")
  //     .option("compression", "snappy")
  //     .parquet(graphFolderPath.resolve("vertices_parquet").toString)

  //   // Save edges with partitioning and compression
  //   edgesDF.write
  //     .mode("overwrite")
  //     .option("compression", "snappy")
  //     .partitionBy("src")
  //     .parquet(graphFolderPath.resolve("edges_parquet").toString)

  //   logger.warn("Graph saved successfully.")
  // }
}
