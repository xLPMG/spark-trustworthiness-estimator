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
    }

    val folderPath = args(0)
    var dictionaryPath = ""
    if (args.length > 1) {
      dictionaryPath = args(1)
    }

    val spark = SparkSession
      .builder()
      .getOrCreate()

    // Read all .xml.bz2 files in the folder into an RDD
    val filesRDD = spark.sparkContext.binaryFiles(s"$folderPath/*.bz2")

    var dictionary: Map[String, Seq[String]] = null

/////////////////////////////////////////////////////////////////////////////////////////
/// DICTIONARY
/////////////////////////////////////////////////////////////////////////////////////////
    val dictionaryFile: Path = Path.of(dictionaryPath).resolve("dictionary.csv")
    // WRITE
    if (!dictionaryPath.isEmpty && !dictionaryFile.toFile.exists()) {
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
    } else if (!dictionaryPath.isEmpty && dictionaryFile.toFile.exists()) {
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
            MinimalRevision(rev.revisionId, rev.timestamp)
          }
        }
        .collectAsMap()
        .toMap

/////////////////////////////////////////////////////////////////////////////////////////
// LINK RESOLUTION
/////////////////////////////////////////////////////////////////////////////////////////
    val resolvedRevisionsRDD = allRevisionsRDD.map { revision =>
      LinkResolver.resolvePageIDsToRevisionIDs(
        revision,
        groupedRevisionsRDD
      )
    }

/////////////////////////////////////////////////////////////////////////////////////////
// GRAPH CREATION
/////////////////////////////////////////////////////////////////////////////////////////
    var revisionGraph =
      GraphCreator.createRevisionGraph(spark, resolvedRevisionsRDD)
    println(s"Number of vertices: ${revisionGraph.vertices.count}")
    println(s"Number of edges: ${revisionGraph.edges.count}")

    spark.stop()
    logger.info(s"Total Time: ${Watch.stopFormatted("Main")}")
  }

}
