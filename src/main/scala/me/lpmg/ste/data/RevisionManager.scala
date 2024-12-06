package me.lpmg.ste.data

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import me.lpmg.ste.types.Types.{
  bitSetToByteArray,
  byteArrayToBitSet,
  TemplateBitPositions
}
import me.lpmg.ste.time.Watch
import java.nio.file.Path
import me.lpmg.ste.data.DataReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.Revision
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZonedDateTime
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.util.collection.BitSet
import me.lpmg.ste.data.TemplateUpdater
import org.apache.spark.graphx.Edge
import me.lpmg.ste.types.EdgeType
import org.apache.spark.rdd.RDD

class RevisionManager(
    spark: SparkSession,
    dumpFolderPath: String,
    dataFolderPath: String
) {
  val logger = Logger(getClass.getName)
  var dateLimit: Long = 0

  /** Sets the date limit for the graph. Only revisions after this date will be
    * included in the graph.
    *
    * @param date
    */
  def setDateLimit(date: ZonedDateTime): Unit = {
    setDateLimit(date.toInstant().toEpochMilli())
  }

  /** Sets the date limit for the graph. Only revisions after this date will be
    * included in the graph.
    *
    * @param date
    */
  def setDateLimit(date: Long): Unit = {
    dateLimit = date
  }

  /** Initializes the graph by reading all revisions from the dump folder.
    *
    * @return
    *   The revision graph
    */
  def retrieveRevisions(): RDD[Revision] = {
    // Read all .xml.bz2 files in the folder into an RDD
    val filesRDD = spark.sparkContext.binaryFiles(s"$dumpFolderPath/*.bz2")
    logger.warn(s"Total files found: ${filesRDD.count()}")

    /////////////////////////////////////////////////////////////////////////////////////////
    // REVISION EXTRACTION
    /////////////////////////////////////////////////////////////////////////////////////////
    val fixedDateLimit = dateLimit
    val allRevisionsRDD = filesRDD
      .flatMap { case (_, pds) =>
        DataReader.getRevisionsFromPDS(
          pds,
          fixedDateLimit
        )
      }
      .cache()

    logger.warn(s"Total Revisions Extracted: ${allRevisionsRDD.count()}")

    // Find oldest revisions and update parent IDs in a distributed way
    val updatedRevisionsRDD = if (fixedDateLimit <= 0) {
      allRevisionsRDD
    } else {
      logger.warn("Setting parent ID of oldest revisions to -1")

      // Group revisions by their page ID and sort by timestamp
      val groupedRevisionsRDD =
        allRevisionsRDD
          .groupBy(_.pageId)
          .mapValues { revisions =>
            // [oldest, ..., newest]
            revisions.toSeq.sortBy(_.timestamp).map { rev =>
              rev.toIdTimestampPair
            }
          }

      // Create an RDD of oldest revision IDs with a marker
      val oldestRevisionsRDD = groupedRevisionsRDD
        .flatMap { case (pageId, revisions) =>
          revisions.headOption.map(rev => (rev._1, true))
        }

      // Use leftOuterJoin to mark oldest revisions
      allRevisionsRDD
        .keyBy(_.revisionId) // Create key-value pairs for join
        .leftOuterJoin(oldestRevisionsRDD)
        .map { case (revId, (revision, isOldest)) =>
          if (isOldest.isDefined) {
            revision.copy(parentId = -1)
          } else {
            revision
          }
        }
        .persist()
    }

    // Clean up cached RDD
    allRevisionsRDD.unpersist()

    /////////////////////////////////////////////////////////////////////////////////////////
    // TEMPLATE TRACKING
    /////////////////////////////////////////////////////////////////////////////////////////
    TemplateUpdater.updateTemplateBitSetsDistributed(updatedRevisionsRDD)
  }
}
