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
    val filesRDD = spark.sparkContext.binaryFiles(s"$dumpFolderPath/*.bz2")

    var dictionary: Map[String, Seq[String]] = null

/////////////////////////////////////////////////////////////////////////////////////////
/// DICTIONARY
/////////////////////////////////////////////////////////////////////////////////////////
    val dictionaryFile: Path = Path.of(dataFolderPath).resolve("dictionary.csv")
    // WRITE
    if (!dataFolderPath.isEmpty && !dictionaryFile.toFile.exists()) {
      logger.info(s"Creating dictionary file at: $dictionaryFile")
      dictionary = filesRDD
        .map { case (_, pds) =>
          DataReader.getDictionaryFromPDS(pds)
        }
        .reduce(_ ++ _)

      val rows = dictionary.map { case (title, values) =>
        Seq(title, values.head, values(1))
      }.toSeq

      val writer = CSVWriter.open(dictionaryFile.toFile())
      writer.writeRow(List("PageTitle", "PageID", "RedirectsTo"))
      writer.writeAll(rows)
      writer.close()
    } else if (!dataFolderPath.isEmpty && dictionaryFile.toFile.exists()) {
      // READ
      logger.info(s"Reading dictionary file from: $dictionaryFile")
      val reader = CSVReader.open(dictionaryFile.toFile())
      dictionary =
        reader.allWithHeaders().foldLeft(Map.empty[String, Seq[String]]) {
          (acc, row) =>
            acc + (row("PageTitle") -> Seq(row("PageID"), row("RedirectsTo")))
        }
      reader.close()
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
            new MinimalRevision(rev.revisionId, rev.timestamp)
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
    allRevisionsRDD.unpersist()

/////////////////////////////////////////////////////////////////////////////////////////
// GRAPH CREATION
/////////////////////////////////////////////////////////////////////////////////////////
    var revisionGraph =
      GraphCreator.createRevisionGraph(spark, resolvedRevisionsRDD)
    println(s"Number of vertices: ${revisionGraph.vertices.count}")
    println(s"Number of edges: ${revisionGraph.edges.count}")

/////////////////////////////////////////////////////////////////////////////////////////
// GRAPH SAVING
/////////////////////////////////////////////////////////////////////////////////////////

    val graphFolderPath = Path.of(dataFolderPath).resolve("graph")
    if (!graphFolderPath.toFile.exists()) {
      graphFolderPath.toFile.mkdirs()
    }
    // Number of partitions for scalability
    val numPartitions = 50

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
