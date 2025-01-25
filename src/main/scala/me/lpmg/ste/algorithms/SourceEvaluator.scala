package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel
import me.lpmg.ste.data.Revision
import me.lpmg.ste.types.TemplateProbabilityVector
import me.lpmg.ste.data.RevisionPair

object SourceEvaluator extends Serializable {

  def evaluateSources(
      revisions: RDD[Revision]
  ): Map[String, (TemplateProbabilityVector, Int)] = {
    evaluateSourcesDistributed(revisions)
      .collect()
      .toMap
  }

  def evaluateSourcesFromPairs(
      revisionPairs: RDD[RevisionPair]
  ): Map[String, (TemplateProbabilityVector, Int)] = {
    evaluateSourcesFromPairsDistributed(revisionPairs)
      .collect()
      .toMap
  }

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

  def evaluateSourcesFromPairsDistributed(
      revisionPairs: RDD[RevisionPair]
  ): RDD[(String, (TemplateProbabilityVector, Int))] = {
    revisionPairs
      .flatMap { revisionPair =>
        // for each source, count in how many pairs it exists and in how many of those
        // it exists in the revision where the template was removed
        val combinedSources =
          (revisionPair.sourcesTemplateAdded ++ revisionPair.sourcesTemplateRemoved).distinct

        combinedSources.map { source =>
          val sourceExistsInRemoved =
            revisionPair.sourcesTemplateRemoved.contains(source)
          if (sourceExistsInRemoved) {
            // source, (existsInRemovedCount, totalPairCount)
            (source, (1, 1))
          } else {
            (source, (0, 1))
          }
        }
      }
      .reduceByKey {
        case (
              (existsInRemovedCount_1, totalPairCount_1),
              (existsInRemovedCount_2, totalPairCount_2)
            ) =>
          (
            existsInRemovedCount_1 + existsInRemovedCount_2,
            totalPairCount_1 + totalPairCount_2
          )
      }
      .mapValues { case (existsInRemovedCount, totalPairCount) =>
        if (totalPairCount == 0) {
          (TemplateProbabilityVector(0.5f, 0.5f), totalPairCount)
        } else {
          (
            TemplateProbabilityVector(
              (totalPairCount - existsInRemovedCount).toFloat / totalPairCount.toFloat,
              existsInRemovedCount.toFloat / totalPairCount.toFloat
            ),
            totalPairCount
          )
        }
      }
  }
}
