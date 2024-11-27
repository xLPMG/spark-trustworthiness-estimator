package me.lpmg.ste.data

import me.lpmg.ste.types.Revision
import org.apache.spark.util.collection.BitSet

object TemplateUpdater {
      def updateTemplateBitSets(
        revisions: Seq[Revision],
        revisionMap: Map[Long, Revision]
    ): Seq[Revision] = {
      revisions.map { revision =>
        val parentTemplatePresence = revisionMap
          .get(revision.parentId)
          .map(_.templatePresence)
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
