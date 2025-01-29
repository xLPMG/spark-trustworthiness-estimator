package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision
import me.lpmg.ste.data.RevisionPair

/** This job counts the number of revision pairs where the sources have changed.
 * Arguments: <data-folder-path> <revisions-folder>‚
  */
object CountSourceChangesJob {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    if (args.length < 1) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the revision pairs folder")
      System.exit(1)
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    // Load the data
    val revisionManager = new RevisionManager(spark, dataFolderPath)
    val revisions: RDD[RevisionPair] =
      revisionManager.loadRevisionPairs(revisionsFolderName)

    //logger.info("Number of revision pairs: " + revisions.count())

    // Count the number of source changes
    // (changed, unchanged)
    val sourceChanges = revisions
      .map { revisionPair =>
        if (
          revisionPair.sourcesTemplateAdded.length != revisionPair.sourcesTemplateRemoved.length
        ) {
          (1, 0)
        } else if (
          revisionPair.sourcesTemplateAdded.toSet == revisionPair.sourcesTemplateRemoved.toSet
        ) {
          (0, 1)
        } else {
          (1, 0)
        }
      }
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    logger.error("Sources changed: " + sourceChanges._1)
    logger.error("Sources unchanged: " + sourceChanges._2)

    spark.stop()
  }

}
