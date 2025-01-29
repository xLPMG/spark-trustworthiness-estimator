package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.sql.SparkSession
import java.nio.file.Path
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.types.Templates.escapeTemplates

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
      logger.error(
        "Please specify the test split revision"
      )
      System.exit(1)
    } else if (args.length < 4) {
      logger.error(
        "Please specify the template"
      )
      System.exit(1)
    }

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)
    val testSplitRevision = args(2).toLong
    val template = args(3)

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions = revisionManager.loadRevisions(revisionsFolderName, false)

    // mask data
    val maskedRevisions = revisions.map { revision =>
      val revisionId = revision.revisionId

      if (revisionId >= testSplitRevision) {
        revision.copy(
          templateAdded = false,
          templateRemoved = false
        )
      } else {
        revision
      }
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    // save to file
    revisionManager.saveRevisionsToFile(
      maskedRevisions,
      s"revisions-masked-$template-$dateString"
    )

    spark.stop()
  }
}
