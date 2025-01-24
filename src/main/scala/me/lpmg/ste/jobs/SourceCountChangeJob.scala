package me.lpmg.ste.jobs

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.data.RevisionManager
import org.apache.spark.rdd.RDD
import me.lpmg.ste.data.Revision

object SourceCountChangeJob {

  def main(args: Array[String]): Unit = {
    val logger = Logger(getClass.getName)

    if (args.length < 1) {
      logger.error("Please specify the data folder path")
      System.exit(1)
    } else if (args.length < 2) {
      logger.error("Please specify the revisions folder")
      System.exit(1)
    }
    val dataFolderPath = args(0)
    val revisionsFolderName = args(1)

    implicit val spark = SparkSession
      .builder()
      .getOrCreate()

    val revisionManager = new RevisionManager(spark, dataFolderPath)
    val revisions: RDD[Revision] =
      revisionManager.loadRevisions(revisionsFolderName)

    val pairedRevisions = revisions
      .keyBy(_.revisionId)
      .join(revisions.keyBy(_.pairId))
      .values

    val (firstMoreSources, secondMoreSources, equalSources) = pairedRevisions
      .map { case (rev1, rev2) =>
        if (rev1.sources.length > rev2.sources.length) (1, 0, 0)
        else if (rev1.sources.length < rev2.sources.length) (0, 1, 0)
        else (0, 0, 1)
      }
      .reduce { (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3) }

    logger.info(s"The first revision has more sources in $firstMoreSources pairs")
    logger.info(s"The second revision has more sources in $secondMoreSources pairs")
    logger.info(s"The revisions have the same number of sources in $equalSources pairs")

    spark.stop()
  }

}
