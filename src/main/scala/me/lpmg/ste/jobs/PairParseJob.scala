package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.data.RevisionManager
import me.lpmg.ste.time.Watch
import org.apache.spark.sql.SparkSession

import java.time.ZoneId
import java.time.ZonedDateTime

object PairParseJob {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)
    Watch.start("parseJob")

    if (args.length < 1) {
      logger.error("Please specify the dump folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 3) {
      logger.error("Please specify the template")
      System.exit(1)
    }

    val dumpFolderPath = args(0)
    val dataFolderPath = args(1)
    val template = args(2)
    val escapedTemplate = template.toLowerCase().replace(" ", "-")

    logger.warn(s"Filtering for template: $template")

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val filesRDD = spark.sparkContext.binaryFiles(s"$dumpFolderPath/*.bz2")
    val revisions = revisionManager.retrieveRevisionPairs(filesRDD, template)

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"
    revisionManager.saveRevisionPairsToFile(revisions, s"revision-pairs-$escapedTemplate-$dateString")

    logger.warn(s"Total Time: ${Watch.stopFormatted("parseJob")}")
    spark.stop()
  }

}
