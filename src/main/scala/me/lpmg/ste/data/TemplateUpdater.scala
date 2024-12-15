package me.lpmg.ste.data

import org.apache.spark.util.collection.BitSet
import org.apache.spark.rdd.RDD

object TemplateUpdater {

  /** Gets the template change BitSets for a revision based on its parent's
    * template presence.
    *
    * @param revisionTemplatePresence
    *   The revision's template presence BitSet
    * @param parentTemplatePresence
    *   The parent revision's template presence BitSet
    * @return
    *   Tuple of templateAdded and templateRemoved BitSets
    */
  def getTemplateChangeBitsets(
      revisionTemplatePresence: BitSet,
      parentTemplatePresence: BitSet
  ): (BitSet, BitSet) = {
    val templateAdded = new BitSet(revisionTemplatePresence.capacity)
    val templateRemoved = new BitSet(revisionTemplatePresence.capacity)

    for (i <- 0 until revisionTemplatePresence.capacity) {
      val isPresent: Boolean = revisionTemplatePresence.get(i)
      val isPresentInParent: Boolean = parentTemplatePresence.get(i)

      // mark as added if present in revision and not present in parent
      if (isPresent && !isPresentInParent) {
        templateAdded.set(i)
      }
      // mark as removed if present in parent and not present in revision
      else if (!isPresent && isPresentInParent) {
        templateRemoved.set(i)
      }
    }

    (templateAdded, templateRemoved)
  }

  /** Updates a single revision's template BitSets based on its parent's
    * template presence
    *
    * @param revision
    *   The revision to update
    * @param parentTemplatePresence
    *   The parent revision's template presence BitSet
    * @return
    *   Updated revision with new templateAdded and templateRemoved BitSets
    */
  def updateRevisionTemplateBitSets(
      revision: Revision,
      parentTemplatePresence: BitSet
  ): Revision = {
    val (templateAdded, templateRemoved) = getTemplateChangeBitsets(
      revision.templatePresence,
      parentTemplatePresence
    )

    revision.copy(
      templateAdded = templateAdded,
      templateRemoved = templateRemoved
    )
  }

  /** Updates the templateAdded and templateRemoved BitSets for a sequence of
    * revisions.
    *
    * @param revisions
    *   revisions to update
    * @param revisionIdToTemplatesPresenceMap
    *   map of revision IDs to revisions
    * @return
    */
  @deprecated("This is done automatically while parsing revisions.", "0.2.0")
  def updateTemplateBitSets(
      revisions: Seq[Revision],
      revisionIdToTemplatesPresenceMap: Map[Long, BitSet]
  ): Seq[Revision] = {
    revisions.map { revision =>
      val parentTemplatePresence = revisionIdToTemplatesPresenceMap
        .getOrElse(
          revision.parentId,
          new BitSet(revision.templatePresence.capacity)
        )

      updateRevisionTemplateBitSets(revision, parentTemplatePresence)
    }
  }

  /** Updates the templateAdded and templateRemoved BitSets for revisions in a
    * distributed way.
    *
    * @param revisionsRDD
    *   RDD of revisions to update
    * @return
    *   RDD of updated revisions
    */
  @deprecated("This is done automatically while parsing revisions.", "0.2.0")
  def updateTemplateBitSetsDistributed(
      revisionsRDD: RDD[Revision]
  ): RDD[Revision] = {
    // create an RDD of revision IDs to their template presence
    val templatePresenceRDD = revisionsRDD
      .map(rev => (rev.revisionId, rev.templatePresence))

    // join revisions with their parent's template presence and update BitSets
    revisionsRDD
      .map(rev => (rev.parentId, rev))
      .leftOuterJoin(templatePresenceRDD)
      .map { case (parentId, (revision, optionalParentTemplates)) =>
        val parentTemplatePresence = optionalParentTemplates.getOrElse(
          new BitSet(revision.templatePresence.capacity)
        )
        updateRevisionTemplateBitSets(revision, parentTemplatePresence)
      }
  }
}
