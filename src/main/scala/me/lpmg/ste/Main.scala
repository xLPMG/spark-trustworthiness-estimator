package me.lpmg.ste

import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.DataReader
import me.lpmg.ste.graph.GraphCreator
import java.nio.file.Path
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision
import org.apache.spark.sql.execution.streaming.Source
import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.broadcast.Broadcast
import com.github.tototoshi.csv.CSVReader
import me.lpmg.ste.data.LinkResolver
import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import me.lpmg.ste.data.MinimalRevision
import org.apache.spark.sql.DataFrame

object Main {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)
    Watch.start("Main")
    if (args.length < 1) {
      logger.error("Please specify the dump folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    }

    val dumpFolderPath = args(0)
    val dataFolderPath = args(1)

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    // Read all .xml.bz2 files in the folder into an RDD
    // val filesRDD = spark.sparkContext.binaryFiles(s"$dumpFolderPath/*.bz2")

    val filesRDD = spark.sparkContext
      .binaryFiles(s"$dumpFolderPath/*.bz2")
      .zipWithIndex()
      .filter(_._2 < 3)
      .map(_._1)

    logger.info(s"Total files found: ${filesRDD.count()}")

    var dictionary: Map[String, (Long, String)] = Map()

/////////////////////////////////////////////////////////////////////////////////////////
/// DICTIONARY
/////////////////////////////////////////////////////////////////////////////////////////
    val dictionaryFile: Path =
      Path.of(dataFolderPath).resolve("dictionary.parquet")
  // WRITE
    if (!dataFolderPath.isEmpty && !dictionaryFile.toFile.exists()) {
      logger.info(s"Creating dictionary file at: $dictionaryFile")
      // value = (filePath: String, fileContent: PortableDataStream)
      dictionary = filesRDD.aggregate(Map.empty[String, (Long, String)])(
        (acc, value) => acc ++ DataReader.getDictionaryFromPDS(value._2),
        (acc1, acc2) => acc1 ++ acc2
      )

      // Convert dictionary to DataFrame
      val dictionaryDF: DataFrame =
        dictionary.toSeq.toDF("PageTitle", "PageID", "RedirectsTo")

      // Write DataFrame to Parquet
      dictionaryDF.write.parquet(dictionaryFile.toString)
    } else if (!dataFolderPath.isEmpty && dictionaryFile.toFile.exists()) {
  // READ
      logger.info(s"Reading dictionary file from: $dictionaryFile")

      // Read DataFrame from Parquet
      val dictionaryDF: DataFrame = spark.read.parquet(dictionaryFile.toString)

      // Convert DataFrame to Map
      dictionary = dictionaryDF
        .collect()
        .map(row => row.getString(0) -> (row.getLong(1), row.getString(2)))
        .toMap
    }
    val broadCastedDictionary = spark.sparkContext.broadcast(dictionary)

/////////////////////////////////////////////////////////////////////////////////////////
// REVISION EXTRACTION
/////////////////////////////////////////////////////////////////////////////////////////
    val allRevisionsRDD = filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionsFromPDS(pds, broadCastedDictionary.value)
      }
      .cache()

    logger.info(s"Total Revisions Extracted: ${allRevisionsRDD.count()}")

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
    var revisionGraph =
      GraphCreator.createRevisionGraph(spark, resolvedRevisionsRDD)
    allRevisionsRDD.unpersist()
    println(s"Number of vertices: ${revisionGraph.vertices.count}")
    println(s"Number of edges: ${revisionGraph.edges.count}")

/////////////////////////////////////////////////////////////////////////////////////////
// GRAPH SAVING
/////////////////////////////////////////////////////////////////////////////////////////

    val graphFolderPath = Path.of(dataFolderPath).resolve("graph")
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

    println("Graph saved successfully.")

    spark.stop()
    logger.info(s"Total Time: ${Watch.stopFormatted("Main")}")
  }

}
