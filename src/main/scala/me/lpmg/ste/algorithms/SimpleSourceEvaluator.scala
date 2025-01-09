package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel
import me.lpmg.ste.data.Revision
import me.lpmg.ste.types.TemplateProbabilityVector

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
  ): Map[String, TemplateProbabilityVector] = {
    evaluateSourcesDistributed(revisions, sourceTemplatePosition)
      .collect()
      .toMap
  }

  /** Evaluates the trust scores of sources in a distributed way. Sources that
    * appear in revisions with a template added, removed, or none are counted
    * and their probabilities are calculated.
    *
    * @param revisions
    *   RDD of revisions
    * @param sourceTemplatePositions
    *   Sequence of template positions
    * @return
    *   RDD of source URLs and their probabilities (added, removed, none)
    */
  def evaluateSourcesDistributed(
      revisions: RDD[Revision],
      sourceTemplatePosition: Byte
  ): RDD[(String, TemplateProbabilityVector)] = {
    revisions
      .flatMap { revision =>
        revision.sources.map { source =>
          val counts =
            if (revision.templateAdded.get(sourceTemplatePosition)) (1, 0, 0)
            else if (revision.templateRemoved.get(sourceTemplatePosition)) (0, 1, 0)
            else (0, 0, 1)

          (source, counts)
        }
      }
      // Aggregate counts for each source
      .reduceByKey {
        case ((added1, removed1, unchanged1), (added2, removed2, unchanged2)) =>
          (added1 + added2, removed1 + removed2, unchanged1 + unchanged2)
      }
      // Apply additive smoothing to circumvent zero probabilities
      .mapValues { case (added, removed, unchanged) =>
        val alpha = 1.0f // Smoothing parameter
        val total = added + removed + unchanged + 3 * alpha
        (added + alpha, removed + alpha, unchanged + alpha)
      }
      // Calculate probabilities
      .mapValues { case (added, removed, unchanged) =>
        val total = added + removed + unchanged
        TemplateProbabilityVector(added / total, removed / total, unchanged / total)
      }
  }
}
