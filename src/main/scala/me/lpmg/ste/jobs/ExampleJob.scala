package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import scala.util.Random

object ExampleJob {
  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    // Generate a large dataset of random floats
    val numElements = 10000000
    val randomFloats = Seq.fill(numElements)(Random.nextFloat())

    // Distribute the dataset across the Spark cluster
    val rdd = spark.sparkContext.parallelize(randomFloats)

    // Perform a computation on the dataset (e.g., calculate the sum of squares)
    val sumOfSquares = rdd.map(x => x * x).reduce(_ + _)

    // Print the result
    logger.info(s"Sum of squares: $sumOfSquares")

    spark.stop()
  }
}
