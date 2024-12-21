package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.collection.BitSet
import java.nio.file.Path

object MaskDataJob {
  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    if (args.length < 1) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the revisions folder name")
      System.exit(1)
    } else if (args.length < 3) {
      logger.error("Please specify the test split revision")
      System.exit(1)
    }

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)
    val testSplit = args(2).toLong

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions = revisionManager.loadRevisions(revisionsFolderName, false)

    // mask data
    val maskedRevisions = revisions.map { revision =>
      // clear template information for test data
      if (revision.revisionId >= testSplit) {
        revision.copy(
          templatePresence = new BitSet(0),
          templateAdded = new BitSet(0),
          templateRemoved = new BitSet(0)
        )
      } else {
        revision
      }
    }

    // save to file
    revisionManager.saveRevisionsToFile(
      maskedRevisions,
      "revisions-masked-" + testSplit.toString()
    )

    spark.stop()
  }
}
