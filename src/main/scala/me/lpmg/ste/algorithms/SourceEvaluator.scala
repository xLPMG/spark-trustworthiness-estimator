package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.Revision
import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.RevisionVertex
import org.apache.spark.storage.StorageLevel

object SourceEvaluator extends Serializable {

  /** Evaluates the trust scores of sources.
    *
    * @param revisions
    *   RDD of revisions
    * @return
    *   Map of source URLs and their trust scores
    */
  def evaluateSources(
      revisions: RDD[Revision],
      sourceTemplatePositions: Seq[Int]
  ): Map[String, Float] = {
    evaluateSourcesDistributed(revisions, sourceTemplatePositions).collect().toMap
  }

  /** Evaluates the trust scores of sources in a distributed way.
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
      sourceTemplatePositions: Seq[Int]
  ): RDD[(String, Float)] = {
    // For each revision, get (source, templateImpact) pairs
    val sourceImpacts = revisions.flatMap { revision =>
      // Only consider specified template positions
      val templateAddedCount = sourceTemplatePositions.count(pos => revision.templateAdded.get(pos))
      val templateRemovedCount = sourceTemplatePositions.count(pos => revision.templateRemoved.get(pos))
      
      // Calculate impact: negative for adding templates, positive for removing
      val templateImpact = (-templateAddedCount + templateRemovedCount).toFloat
      
      revision.sources.map(source => (source, templateImpact))
    }.persist(StorageLevel.MEMORY_AND_DISK)

    // find maximum absolute value for normalization
    val maxAbsOpt = sourceImpacts
      .map { case (_, score) => math.abs(score) }
      .takeOrdered(1)
      .headOption
      .getOrElse(0.0f)

    // normalize scores and combine impacts for each source
    val normalizedSourceScores = sourceImpacts
      .mapValues(score =>
        if (maxAbsOpt > 0) score / maxAbsOpt
        else score
      )
      .reduceByKey(_ + _)
      .mapValues(sum => math.max(-1.0f, math.min(1.0f, sum)))

    normalizedSourceScores
  }



}
