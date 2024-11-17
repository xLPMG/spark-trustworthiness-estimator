package me.lpmg.ste

import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.DataReader
import me.lpmg.ste.graph.GraphCreator.createRevisionGraph
import java.nio.file.Path
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision
import org.apache.spark.sql.execution.streaming.Source
import com.github.tototoshi.csv.CSVWriter

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Please specify the dump folder path")
      System.exit(1)
    }

    val folderPath = args(0)
    var dictionaryPath = ""
    if (args.length > 1) {
      dictionaryPath = args(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Spark Trustworthiness Estimator")
      .getOrCreate()

    // Read all .xml.bz2 files in the folder into an RDD
    val filesRDD = spark.sparkContext.binaryFiles(s"$folderPath/*.bz2")

    // Create dictionary if not present
    val dictionaryFile: Path = Path.of(dictionaryPath).resolve("dictionary.csv")
    if (!dictionaryPath.isEmpty && !dictionaryFile.toFile.exists()) {
      val dictionaryRDD = filesRDD.map { case (_, pds) =>
        DataReader.getDictionaryFromPDS(pds)
      }
      val combinedDictionary = dictionaryRDD.reduce(_ ++ _)
      val rows = combinedDictionary.map { case (title, values) =>
        Seq(title, values.head, values(1))
      }.toSeq
      val writer = CSVWriter.open(dictionaryFile.toFile())
      writer.writeAll(rows)
      writer.close()
    }

    // Process each file in the RDD to extract revisions
    val allRevisionsRDD = filesRDD.flatMap { case (_, pds) =>
      DataReader.getRevisionsFromPDS(pds)
    }

    println(s"Total Revisions Extracted: ${allRevisionsRDD.count()}")

    // Create the graph
    val revisionGraph = createRevisionGraph(spark, allRevisionsRDD)

    println(s"Number of vertices: ${revisionGraph.vertices.count}")
    println(s"Number of edges: ${revisionGraph.edges.count}")

    spark.stop()
  }

}
