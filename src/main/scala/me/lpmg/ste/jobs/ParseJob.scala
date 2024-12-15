package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.graph.GraphManager
import org.apache.spark.graphx._
import me.lpmg.ste.types.Revision
import me.lpmg.ste.algorithms.TrustCalculator
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.types.RevisionVertex
import me.lpmg.ste.algorithms.ContributorEvaluator
import me.lpmg.ste.data.RevisionManager
import me.lpmg.ste.graph.GraphCreator
import javax.xml.transform.Source
import me.lpmg.ste.algorithms.SourceEvaluator
import java.nio.file.Path
import org.apache.spark.storage.StorageLevel

object ParseJob {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)
    Watch.start("parseJob")

    if (args.length < 1) {
      logger.error("Please specify the dump folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    }

    val dumpFolderPath = args(0)
    val dataFolderPath = args(1)

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val filesRDD = spark.sparkContext.binaryFiles(s"$dumpFolderPath/*.bz2")
    val revisions = revisionManager.retrieveRevisions(filesRDD)

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"
    revisionManager.saveRevisionsToFile(revisions, s"revisions-$dateString")

    logger.warn(s"Total Time: ${Watch.stopFormatted("parseJob")}")
    spark.stop()
  }

}
