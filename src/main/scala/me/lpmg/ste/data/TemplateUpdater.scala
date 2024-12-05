package me.lpmg.ste.data

import me.lpmg.ste.types.Revision
import org.apache.spark.util.collection.BitSet

object TemplateUpdater {

  /** Updates the templateAdded and templateRemoved BitSets for a sequence of
    * revisions.
    *
    * @param revisions
    *   revisions to update
    * @param revisionIdToTemplatesPresenceMap
    *   map of revision IDs to revisions
    * @return
    */
  def updateTemplateBitSets(
      revisions: Seq[Revision],
      revisionIdToTemplatesPresenceMap: Map[Long, BitSet]
  ): Seq[Revision] = {
    revisions.map { revision =>
      val parentTemplatePresence = revisionIdToTemplatesPresenceMap
        .get(revision.parentId)
        .getOrElse(new BitSet(revision.templatePresence.capacity))

      val templateAdded = new BitSet(revision.templatePresence.capacity)
      val templateRemoved = new BitSet(revision.templatePresence.capacity)

      for (i <- 0 until revision.templatePresence.capacity) {
        if (
          revision.templatePresence.get(i) && !parentTemplatePresence.get(i)
        ) {
          templateAdded.set(i)
        }
        if (
          !revision.templatePresence.get(i) && parentTemplatePresence.get(i)
        ) {
          templateRemoved.set(i)
        }
      }

      revision.copy(
        templateAdded = templateAdded,
        templateRemoved = templateRemoved
      )
    }
  }
}
