package me.lpmg.ste

import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.DataReader
import me.lpmg.ste.graph.GraphCreator.createRevisionGraph

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Please specify the dump folder path")
      System.exit(1)
    }

    val folderPath = args(0)

    val spark = SparkSession
      .builder()
      .appName("Spark Trustworthiness Estimator")
      .getOrCreate()

    // Read all .xml.bz2 files in the folder into an RDD
    val filesRDD = spark.sparkContext.binaryFiles(s"$folderPath/*.bz2")

    // Process each file in the RDD to extract revisions
    val allRevisionsRDD = filesRDD.flatMap { case (path, _) =>
      DataReader.parseXMLFile(path)
    }

    println(s"Total Revisions Extracted: ${allRevisionsRDD.count()}")

    // Create the graph
    val revisionGraph = createRevisionGraph(spark, allRevisionsRDD)

    println(s"Number of vertices: ${revisionGraph.vertices.count}")
    println(s"Number of edges: ${revisionGraph.edges.count}")

    spark.stop()
  }

}
