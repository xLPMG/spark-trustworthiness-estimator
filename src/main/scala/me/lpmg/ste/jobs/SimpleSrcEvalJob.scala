package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import me.lpmg.ste.time.Watch
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.algorithms.SimpleSourceEvaluator
import java.nio.file.Path
import java.time.ZonedDateTime
import java.time.ZoneId
import me.lpmg.ste.types.Types
import scala.collection.mutable

object SimpleSrcEvalJob {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    if (args.length < 1) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the revisions folder name")
      System.exit(1)
    }

    val date = ZonedDateTime.now(ZoneId.of("UTC"))
    val dateString = date.toString().replace(":", "-").split("\\.")(0) + "Z"

    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)
    var testSplit: Long = 0L
    if (args.length > 2) {
      testSplit = args(2).toLong
    }

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager =
      new RevisionManager(spark, dataFolderPath)

    val revisions = revisionManager.loadRevisions(revisionsFolderName, false)

    // SOURCE SCORE COMPUTATION

    val sourceScores_UNREFERENCED = SimpleSourceEvaluator.evaluateSources(
      revisions,
      getBit("Unreferenced")
    )

    val sourceScores_ONE_SOURCE = SimpleSourceEvaluator.evaluateSources(
      revisions,
      getBit("One source")
    )

    val sourceScores_ORIGINAL_RESEARCH = SimpleSourceEvaluator.evaluateSources(
      revisions,
      getBit("Original research")
    )

    val sourceScores_MORE_CITATIONS_NEEDED =
      SimpleSourceEvaluator.evaluateSources(
        revisions,
        getBit("More citations needed")
      )

    val sourceScores_DISPUTED = SimpleSourceEvaluator.evaluateSources(
      revisions,
      getBit("Disputed")
    )

    val sourceScores_POV = SimpleSourceEvaluator.evaluateSources(
      revisions,
      getBit("POV")
    )

    val sourceScores_THIRD_PARTY = SimpleSourceEvaluator.evaluateSources(
      revisions,
      getBit("Third-party")
    )

    val sourceScores_CONTRADICT = SimpleSourceEvaluator.evaluateSources(
      revisions,
      getBit("Contradict")
    )

    val sourceScores_HOAX = SimpleSourceEvaluator.evaluateSources(
      revisions,
      getBit("Hoax")
    )

    // Calculate likelihoods and filter out revisions with no likelihoods
    val templateLikelihoods = revisions
      .map { revision =>
        var likelihoodMap: mutable.Map[Byte, Float] = mutable.Map().empty
        val minimumValue = 0.001f

        // UNREFERENCED
        val likelihood_UNREFERENCED = revision.sources
          .map(source => sourceScores_UNREFERENCED.getOrElse(source, 0.0f))
          .sum
        if (Math.abs(likelihood_UNREFERENCED) > minimumValue) {
          likelihoodMap += (getBit("Unreferenced") -> sig(likelihood_UNREFERENCED))
        }

        // ONE SOURCE
        val likelihood_ONE_SOURCE = revision.sources
          .map(source => sourceScores_ONE_SOURCE.getOrElse(source, 0.0f))
          .sum
        if (Math.abs(likelihood_ONE_SOURCE) > minimumValue) {
          likelihoodMap += (getBit("One source") -> sig(likelihood_ONE_SOURCE))
        }

        // ORIGINAL RESEARCH
        val likelihood_ORIGINAL_RESEARCH = revision.sources
          .map(source => sourceScores_ORIGINAL_RESEARCH.getOrElse(source, 0.0f))
          .sum
        if (Math.abs(likelihood_ORIGINAL_RESEARCH) > minimumValue) {
          likelihoodMap += (getBit(
            "Original research"
          ) -> sig(likelihood_ORIGINAL_RESEARCH))
        }

        // MORE CITATIONS NEEDED
        val likelihood_MORE_CITATIONS_NEEDED = revision.sources
          .map(source =>
            sourceScores_MORE_CITATIONS_NEEDED.getOrElse(source, 0.0f)
          )
          .sum
        if (Math.abs(likelihood_MORE_CITATIONS_NEEDED) > minimumValue) {
          likelihoodMap += (getBit(
            "More citations needed"
          ) -> sig(likelihood_MORE_CITATIONS_NEEDED))
        }

        // DISPUTED
        val likelihood_DISPUTED = revision.sources
          .map(source => sourceScores_DISPUTED.getOrElse(source, 0.0f))
          .sum
        if (Math.abs(likelihood_DISPUTED) > minimumValue) {
          likelihoodMap += (getBit("Disputed") -> sig(likelihood_DISPUTED))
        }

        // POV
        val likelihood_POV = revision.sources
          .map(source => sourceScores_POV.getOrElse(source, 0.0f))
          .sum
        if (Math.abs(likelihood_POV) > minimumValue) {
          likelihoodMap += (getBit("POV") -> sig(likelihood_POV))
        }

        // THIRD PARTY
        val likelihood_THIRD_PARTY = revision.sources
          .map(source => sourceScores_THIRD_PARTY.getOrElse(source, 0.0f))
          .sum
        if (Math.abs(likelihood_THIRD_PARTY) > minimumValue) {
          likelihoodMap += (getBit("Third-party") -> sig(likelihood_THIRD_PARTY))
        }

        // CONTRADICT
        val likelihood_CONTRADICT = revision.sources
          .map(source => sourceScores_CONTRADICT.getOrElse(source, 0.0f))
          .sum
        if (Math.abs(likelihood_CONTRADICT) > minimumValue) {
          likelihoodMap += (getBit("Contradict") -> sig(likelihood_CONTRADICT))
        }

        // HOAX
        val likelihood_HOAX = revision.sources
          .map(source => sourceScores_HOAX.getOrElse(source, 0.0f))
          .sum
        if (Math.abs(likelihood_HOAX) > minimumValue) {
          likelihoodMap += (getBit("Hoax") -> sig(likelihood_HOAX))
        }

        // Return tuple with revision ID and likelihoods as immutable map
        (revision.revisionId, likelihoodMap.toMap)
      }
      .filter(_._2.nonEmpty)

    import spark.implicits._

    val likelihoodsOutputPath =
      Path
        .of(dataFolderPath)
        .resolve(s"simple-template-likelihoods-$dateString")

    val likelihoodsDF = templateLikelihoods
      .map { case (revisionId, likelihoods) =>
        (
          revisionId,
          likelihoods.getOrElse(
            getBit("Unreferenced"),
            null.asInstanceOf[Float]
          ),
          likelihoods.getOrElse(getBit("One source"), null.asInstanceOf[Float]),
          likelihoods.getOrElse(
            getBit("Original research"),
            null.asInstanceOf[Float]
          ),
          likelihoods.getOrElse(
            getBit("More citations needed"),
            null.asInstanceOf[Float]
          ),
          likelihoods.getOrElse(getBit("Disputed"), null.asInstanceOf[Float]),
          likelihoods.getOrElse(getBit("POV"), null.asInstanceOf[Float]),
          likelihoods.getOrElse(
            getBit("Third-party"),
            null.asInstanceOf[Float]
          ),
          likelihoods.getOrElse(getBit("Contradict"), null.asInstanceOf[Float]),
          likelihoods.getOrElse(getBit("Hoax"), null.asInstanceOf[Float])
        )
      }
      .toDF(
        "revisionId",
        "lh_unreferenced",
        "lh_one_source",
        "lh_original_research",
        "lh_more_citations_needed",
        "lh_disputed",
        "lh_pov",
        "lh_third_party",
        "lh_contradict",
        "lh_hoax"
      )

    likelihoodsDF.write
      .mode("overwrite")
      .option("header", "true")
      .option("nullValue", "")
      .csv(likelihoodsOutputPath.toString)

    logger.warn(
      s"CSV file saved: ${likelihoodsOutputPath.toString()}"
    )

    spark.stop()
  }

  private def getBit(templateName: String): Byte = {
    Types.TemplateBitPositions.getOrElse(templateName, 0.toByte)
  }

  private def sig(value: Float): Float = {
    1.0f / (1.0f + math.exp(-value).toFloat)
  }

}
