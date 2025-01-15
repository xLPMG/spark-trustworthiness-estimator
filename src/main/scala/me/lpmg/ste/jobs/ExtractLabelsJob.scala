package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.data.RevisionManager
import me.lpmg.ste.types.Types.TemplateBitPositions
import org.apache.spark.util.collection.BitSet

object ExtractLabelsJob {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    if (args.length < 1) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the revisions folder")
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
    val revisions = revisionManager.loadRevisions(revisionsFolderName)

    import java.nio.file.{Files, Paths}

    val labelsFolderName = s"labels-$dateString"
    val labelsFolderPath = Paths.get(dataFolderPath, labelsFolderName)
    Files.createDirectories(labelsFolderPath)

    val templateBitPositions = TemplateBitPositions.map {
      case (template, position) =>
        val escapedTemplate = template.toLowerCase.replace(" ", "-")
        escapedTemplate -> position
    }

    templateBitPositions.foreach { case (template, position) =>
      val templateFilePath = labelsFolderPath.resolve(s"$template")
      val templateData = revisions.map { revision =>

        if(revision.templateAddedGT.get(position)) {
          (revision.revisionId, 1.0f)
        } else if (revision.templateRemovedGT.get(position)) {
          (revision.revisionId, 0.0f)
        } else {
          (revision.revisionId, -1.0f)
        }
      }
      .filter(_._2 != -1.0f)
      .toDF("revision_id", "has_template")

      templateData.write
        .mode("overwrite")
        .option("header", "true")
        .csv(templateFilePath.toString)
    }

    spark.stop()
  }

}
