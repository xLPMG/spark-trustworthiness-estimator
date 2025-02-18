package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.algorithms.ProbabilityHandler
import me.lpmg.ste.algorithms.SourceEvaluator
import me.lpmg.ste.data.Revision
import me.lpmg.ste.data.RevisionManager
import me.lpmg.ste.data.RevisionPair
import me.lpmg.ste.data.TemplateProbabilityVector
import me.lpmg.ste.time.Watch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import java.nio.file.Path
import java.time.ZoneId
import java.time.ZonedDateTime

/** This job evaluates the sources of revision pairs. Unchanged sources are
  * included in the evaluation. Arguments: <data-folder-path> <revisions-folder>
  * <template> <test-split-revision>
  */
object PairSourceUnchangedEvalJob {

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
    val revisions: RDD[RevisionPair] = revisionManager
      .loadRevisionPairs(revisionsFolderName)
      .filter(_.revisionIdTemplateAdded < testSplitRevision)
    val sourceProbabilities = SourceEvaluator
      .evaluateSourcesFromPairsWithUnchangedSources(revisions)
      // only include sources that appeared in revisions where a template was added or removed
      // this shouldnt be necessary though since the revisions folder only
      // contains revisions where a template was added or removed
      .filter(_._2._2 > 0)

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

    val sourceProbabilitiesOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"source-pair-wU-probabilities-$escapedTemplate-$dateString")

    val sourceProbabilitiesDF = sourceProbabilities
      .map { case (source, probabilities) =>
        val rounded1 = BigDecimal.valueOf(probabilities._1._1.toDouble).setScale(
          4,
          BigDecimal.RoundingMode.HALF_UP
        )

        val rounded3 = BigDecimal.valueOf(probabilities._1._3.toDouble).setScale(
          4,
          BigDecimal.RoundingMode.HALF_UP
        )

        val rounded2 =
          (BigDecimal.valueOf(1.0) - rounded1 - rounded3)
            .setScale(4, BigDecimal.RoundingMode.HALF_UP)

        (
          source,
          rounded1,
          rounded2,
          rounded3,
          probabilities._2
        )
      }
      .toSeq
      .toDF(
        "src",
        "probabilityTemplateAdded",
        "probabilityTemplateRemoved",
        "probabilityTemplateUnchanged",
        "occurences"
      )

    sourceProbabilitiesDF.write
      .option("header", "true")
      .csv(sourceProbabilitiesOutputPath.toString)

    spark.stop()
  }
}
