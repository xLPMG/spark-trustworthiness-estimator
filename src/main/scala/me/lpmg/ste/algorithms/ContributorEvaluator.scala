package me.lpmg.ste.algorithms

import org.apache.spark.rdd.RDD
import me.lpmg.ste.types.Revision
import org.apache.spark.graphx.Graph
import me.lpmg.ste.types.RevisionVertex

object ContributorEvaluator extends Serializable {

  /** Evaluates the trust scores of contributors.
    *
    * @param revisions
    *   RDD of revisions
    * @return
    *   Map of contributor IDs and their trust scores
    */
  def evaluateContributors(
      revisions: RDD[Revision]
  ): Map[Int, Float] = {
    evaluateContributorsDistributed(revisions, Seq.empty).collect().toMap
  }

  /** Evaluates the trust scores of contributors in a distributed way.
    *
    * @param revisions
    *   RDD of revisions
    * @param contributorTemplatePositions
    *   Sequence of template positions related to contributor trustworthiness
    * @return
    *   RDD of contributor IDs and their trust scores
    */
  def evaluateContributorsDistributed(
      revisions: RDD[Revision],
      contributorTemplatePositions: Seq[Int]
  ): RDD[(Int, Float)] = {
    // Create an RDD of revision ID to contributor ID mapping
    val revisionContributors = revisions.map(rev => (rev.revisionId, rev.contributorId))

    // For each revision, get (contributor, templateImpact) pairs
    val contributorImpacts = revisions
      .map { revision =>
        // Only consider specified template positions
        val templateAddedCount = contributorTemplatePositions.count(pos =>
          revision.templateAdded.get(pos)
        )
        val templateRemovedCount = contributorTemplatePositions.count(pos =>
          revision.templateRemoved.get(pos)
        )

        // Calculate impact: negative for adding templates, positive for removing
        val templateImpact =
          (-templateAddedCount + templateRemovedCount).toFloat

        // If there are template changes, we'll need to look up the parent's contributor
        val hasTemplateChanges = templateAddedCount > 0 || templateRemovedCount > 0
        
        (revision.revisionId, (revision.contributorId, templateImpact, hasTemplateChanges, revision.parentId))
      }
      // Join with parent revisions to get their contributor IDs when needed
      .map { case (revId, (contributorId, impact, hasTemplateChanges, parentId)) =>
        if (hasTemplateChanges && parentId != -1) {
          (parentId, (contributorId, impact, true)) // We'll look up parent's contributor
        } else {
          (revId, (contributorId, impact, false)) // Use current contributor
        }
      }
      .leftOuterJoin(revisionContributors)
      .map { case (_, ((originalContributorId, impact, needsParentContributor), parentContributorIdOpt)) =>
        val finalContributorId = if (needsParentContributor) {
          parentContributorIdOpt.getOrElse(originalContributorId)
        } else {
          originalContributorId
        }
        (finalContributorId, impact)
      }
      .filter(_._1 != -1) // Filter out anonymous contributors
      .cache()

    // find maximum absolute value for normalization
    val maxAbs = contributorImpacts
      .map { case (_, score) => math.abs(score) }
      .max()

    // normalize scores and combine impacts for each contributor
    val normalizedContributorScores = contributorImpacts
      .mapValues(score =>
        if (maxAbs > 0) score / maxAbs
        else score
      )
      .reduceByKey(_ + _)
      .mapValues(sum => math.max(-1.0f, math.min(1.0f, sum)))

    normalizedContributorScores
  }
}
