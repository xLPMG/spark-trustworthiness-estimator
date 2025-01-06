package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel
import me.lpmg.ste.data.Revision

object SimpleSourceEvaluator extends Serializable {

  /** Evaluates the trust scores of sources.
    *
    * @param revisions
    *   RDD of revisions
    * @return
    *   Map of source URLs and their trust scores
    */
  def evaluateSources(
      revisions: RDD[Revision],
      sourceTemplatePosition: Byte
  ): Map[String, Float] = {
    evaluateSourcesDistributed(revisions, sourceTemplatePosition)
      .collect()
      .toMap
  }

  /** Evaluates the trust scores of sources in a distributed way. Sources that
    * appear in revisions with a template added are given higher score than
    * sources that appear in revisions with a template removed.
    *
    * @param revisions
    *   RDD of revisions
    * @param sourceTemplatePositions
    *   Sequence of template positions
    * @return
    *   RDD of source URLs and their trust scores
    */
  def evaluateSourcesDistributed(
      revisions: RDD[Revision],
      sourceTemplatePosition: Byte
  ): RDD[(String, Float)] = {
    revisions
      .flatMap { revision =>
        revision.sources.map { source =>
          val score =
            if (revision.templateRemoved.get(sourceTemplatePosition)) -1.0f
            else if (revision.templateAdded.get(sourceTemplatePosition)) 1.0f
            else 0.0f

          (source, score)
        }
      }
      .reduceByKey(_ + _)
      .filter { case (_, score) => score > 0.001f || score < -0.001f }
  }
}
