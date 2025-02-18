package me.lpmg.ste.algorithms

import me.lpmg.ste.data.AdditionalSourceCleanup
import me.lpmg.ste.data.Revision
import me.lpmg.ste.data.RevisionPair
import me.lpmg.ste.data.TemplateProbabilityVector
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/** This object provides functions to evaluate external sources based on the
  * revisions they appear in
  */
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

  def evaluateSourcesFromPairsWithoutUnchangedSources(
      revisionPairs: RDD[RevisionPair]
  ): Map[String, (TemplateProbabilityVector, Int)] = {
    evaluateSourcesFromPairsWithoutUnchangedSourcesDistributed(revisionPairs)
      .collect()
      .toMap
  }

  def evaluateSourcesFromPairsWithoutUnchangedSourcesDistributed(
      revisionPairs: RDD[RevisionPair]
  ): RDD[(String, (TemplateProbabilityVector, Int))] = {
    revisionPairs
      .flatMap { revisionPair =>
        val cleanedAddedSources = revisionPair.sourcesTemplateAdded.map {
          source =>
            AdditionalSourceCleanup.cleanupSource(source)
        }
        val cleanedRemovedSources = revisionPair.sourcesTemplateRemoved.map {
          source =>
            AdditionalSourceCleanup.cleanupSource(source)
        }

        val combinedSources =
          (cleanedAddedSources ++ cleanedRemovedSources).distinct

        combinedSources.map { source =>
          val sourceExistsWhenTemplateRemoved =
            cleanedRemovedSources.contains(source)
          val sourceExistsWhenTemplateAdded =
            cleanedAddedSources.contains(source)

          if (
            sourceExistsWhenTemplateAdded && !sourceExistsWhenTemplateRemoved
          ) {
            // source was removed when template was removed
            (source, (1, 0))
          } else if (
            !sourceExistsWhenTemplateAdded && sourceExistsWhenTemplateRemoved
          ) {
            // source was added when template was removed
            (source, (0, 1))
          } else {
            // unchanged source
            (source, (0, 0))
          }
        }
      }
      .reduceByKey {
        case (
              (addedCount_1, removedCount_1),
              (addedCount_2, removedCount_2)
            ) =>
          (
            addedCount_1 + addedCount_2,
            removedCount_1 + removedCount_2
          )
      }
      .mapValues { case (addedCount, removedCount) =>
        val totalPairCount = addedCount + removedCount
        if (totalPairCount == 0) {
          (TemplateProbabilityVector(0.5f, 0.5f), totalPairCount)
        } else {
          (
            TemplateProbabilityVector(
              addedCount.toFloat / totalPairCount.toFloat,
              removedCount.toFloat / totalPairCount.toFloat
            ),
            totalPairCount
          )
        }
      }
  }

  def evaluateSourcesFromPairsWithUnchangedSources(
      revisionPairs: RDD[RevisionPair]
  ): Map[String, ((Float, Float, Float), Int)] = {
    evaluateSourcesFromPairsWithUnchangedSourcesDistributed(revisionPairs)
      .collect()
      .toMap
  }

  def evaluateSourcesFromPairsWithUnchangedSourcesDistributed(
      revisionPairs: RDD[RevisionPair]
  ): RDD[(String, ((Float, Float, Float), Int))] = {
    revisionPairs
      .flatMap { revisionPair =>
        val combinedSources =
          (revisionPair.sourcesTemplateAdded ++ revisionPair.sourcesTemplateRemoved).distinct

        combinedSources.map { source =>
          val sourceExistsWhenTemplateRemoved =
            revisionPair.sourcesTemplateRemoved.contains(source)
          val sourceExistsWhenTemplateAdded =
            revisionPair.sourcesTemplateAdded.contains(source)

          if (
            sourceExistsWhenTemplateRemoved && sourceExistsWhenTemplateAdded
          ) {
            // unchanged source
            (source, (0, 0, 1))
          } else if (sourceExistsWhenTemplateRemoved) {
            // source was added when template was removed
            (source, (0, 1, 0))
          } else if (sourceExistsWhenTemplateAdded) {
            // source was removed when template was added
            (source, (1, 0, 0))
          } else {
            // should not happen
            (source, (0, 0, 0))
          }
        }
      }
      .reduceByKey {
        case (
              (addedCount_1, removedCount_1, unchangedCount_1),
              (addedCount_2, removedCount_2, unchangedCount_2)
            ) =>
          (
            addedCount_1 + addedCount_2,
            removedCount_1 + removedCount_2,
            unchangedCount_1 + unchangedCount_2
          )
      }
      .mapValues { case (addedCount, removedCount, unchangedCount) =>
        val totalPairCount = addedCount + removedCount + unchangedCount
        if (totalPairCount == 0) {
          // should not happen
          ((0.33f, 0.33f, 0.34f), totalPairCount)
        } else {
          (
            (
              addedCount.toFloat / totalPairCount.toFloat,
              removedCount.toFloat / totalPairCount.toFloat,
              unchangedCount.toFloat / totalPairCount.toFloat
            ),
            totalPairCount
          )
        }
      }
  }
}
