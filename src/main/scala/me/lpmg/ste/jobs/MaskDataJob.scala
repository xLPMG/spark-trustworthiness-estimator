package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.collection.BitSet
import java.nio.file.Path
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.types.Types.{TemplateBitPositions, escapeTemplates}

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
        "Please specify the splits for the revisions for each template"
      )
      System.exit(1)
    }

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)
    val testSplitString = args(2)

    // example: pov:1221;one-source:1231233;disputed:123123
    val escapedTemplateBitPositions = escapeTemplates(TemplateBitPositions)
    val templateBitPositionToSplits = testSplitString
      .split(";")
      .map { templateSplit =>
        val split = templateSplit.split(":")
        val templatePosition =
          escapedTemplateBitPositions.getOrElse(split(0), -1.toByte)
        templatePosition -> split(1).toLong
      }
      .toMap
      // filter out templates that could not be found
      .filter(_._1 != -1.toByte)

      templateBitPositionToSplits.foreach { case (templatePosition, split) =>
        println(s"Template Position: $templatePosition, Split: $split")
      }

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions = revisionManager.loadRevisions(revisionsFolderName, false)

    // mask data
    val maskedRevisions = revisions.map { revision =>
      // clear template information for test data
      var templateAddedMasked = revision.templateAdded.&(revision.templateAdded)
      var templateRemovedMasked = revision.templateRemoved.&(revision.templateRemoved)
      var templatePresentMasked = revision.templatePresence.&(revision.templatePresence)
      val revisionId = revision.revisionId

      templateBitPositionToSplits.foreach { case (templatePosition, split) =>
        if (revisionId >= split) {
          templateAddedMasked.unset(templatePosition)
          templateRemovedMasked.unset(templatePosition)
          templatePresentMasked.unset(templatePosition)
        }
      }

      revision.copy(
        templateAdded = templateAddedMasked,
        templateRemoved = templateRemovedMasked,
        templatePresence = templatePresentMasked
      )
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    // save to file
    revisionManager.saveRevisionsToFile(
      maskedRevisions,
      "revisions-masked-" + dateString
    )

    spark.stop()
  }
}
