package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel
import me.lpmg.ste.data.Revision
import me.lpmg.ste.types.TemplateProbabilityVector

object SourceEvaluator extends Serializable {

  /** Evaluates the trust scores of sources.
    *
    * @param revisions
    *   RDD of revisions
    * @return
    *   Map of source URLs and their trust scores
    */
  def evaluateSources(
      revisions: RDD[Revision]
  ): Map[String, (TemplateProbabilityVector, Int)] = {
    evaluateSourcesDistributed(revisions)
      .collect()
      .toMap
  }

  /** Evaluates the trust scores of sources in a distributed way. Sources that
    * appear in revisions with a template added, removed, or none are counted
    * and their probabilities are calculated.
    *
    * @param revisions
    *   RDD of revisions
    * @return
    *   RDD of source URLs and their probabilities (added, removed)
    */
  def evaluateSourcesDistributed(
      revisions: RDD[Revision]
  ): RDD[(String, (TemplateProbabilityVector, Int))] = {
    revisions
      .flatMap { revision =>
        revision.sources.map { source =>
          val counts =
            if (revision.templateAdded) (1, 0)
            else if (revision.templateRemoved) (0, 1)
            else (0, 0)

          (source, counts)
        }
      }
      // Aggregate counts for each source
      .reduceByKey { case ((added1, removed1), (added2, removed2)) =>
        (added1 + added2, removed1 + removed2)
      }
      .mapValues { case (added, removed) =>
        val total = added + removed

        if (total == 0) {
          (TemplateProbabilityVector(0.5f, 0.5f), total)
        } else {
          (
            TemplateProbabilityVector(
              added.toFloat / total.toFloat,
              removed.toFloat / total.toFloat
            ),
            total
          )
        }
      }
  }
}
