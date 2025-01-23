package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision

object ExtractLabelsJob {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    if (args.length < 1) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the revisions folder")
      System.exit(1)
    } else if (args.length < 3) {
      logger.error("Please specify the template")
      System.exit(1)
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)
    val template = args(2)
    val escapedTemplate = template.toLowerCase.replace(" ", "-")

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    // Load the data
    val revisionManager = new RevisionManager(spark, dataFolderPath)
    val revisions: RDD[Revision] =
      revisionManager.loadRevisions(revisionsFolderName)

    import java.nio.file.{Files, Paths}

    val labelsFolderName = s"labels-$escapedTemplate-$dateString"
    val labelsFolderPath = Paths.get(dataFolderPath, labelsFolderName)
    Files.createDirectories(labelsFolderPath)

    val templateData = revisions
      .map { revision =>
        if (revision.templateAddedGT) {
          (revision.revisionId, revision.pairId, 1.0f)
        } else if (revision.templateRemovedGT) {
          (revision.revisionId, revision.pairId, 0.0f)
        } else {
          // should not happen
          (revision.revisionId, revision.pairId, -1.0f)
        }
      }
      // only keep added and removed data
      .filter(_._3 >= 0.0f)
      .toDF("revision_id", "pair_id", "has_template")

    templateData.write
      .mode("overwrite")
      .option("header", "true")
      .csv(labelsFolderPath.toString)

    spark.stop()
  }

}
