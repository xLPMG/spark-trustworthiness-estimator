package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.sql.{SparkSession, DataFrame, Column}
import org.apache.spark.sql.functions.{col, expr, when}
import scala.collection.mutable
import me.lpmg.ste.algorithms.SourceEvaluator
import java.nio.file.Path
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.types.Types.TemplateBitPositions
import org.apache.spark.sql.Row
import me.lpmg.ste.types.TemplateProbabilityVector
import me.lpmg.ste.algorithms.ProbabilityHandler
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision

object SourceEvalJob {

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
        "Please specify the template name"
      )
      System.exit(1)
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)
    val template = args(2)
    val escapedTemplate = template.toLowerCase().replace(" ", "-")

    var testSplitRevision = Long.MaxValue
    if (args.length > 3) {
      try {
        testSplitRevision = args(3).toLong
      } catch {
        case e: NumberFormatException =>
          logger.error("Invalid test split revision number")
          System.exit(1)
      }
    }

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    // only load revisions up to the test split revision for source evaluation
    val revisions: RDD[Revision] = revisionManager
      .loadRevisions(revisionsFolderName)
      .filter(_.revisionId < testSplitRevision)
    val sourceProbabilities = SourceEvaluator.evaluateSources(revisions)

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    val sourceProbabilitiesOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"source-probabilities-$escapedTemplate-$dateString")

    val sourceProbabilitiesDF = sourceProbabilities
      .map { case (source, probabilities) =>
        val probabilitiesString =
          if (probabilities.isUndecided) null
          else probabilities.extractValuesString
        (source, probabilitiesString)
      }
      .toSeq
      .toDF("src", "probability")

    sourceProbabilitiesDF
      .write
      .option("header", "true")
      .csv(sourceProbabilitiesOutputPath.toString)

    spark.stop()
  }
}
