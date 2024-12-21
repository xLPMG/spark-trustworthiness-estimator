package me.lpmg.ste.rules

import org.apache.spark.util.collection.BitSet
import me.lpmg.ste.data.Revision

object RevisionTestRule {
  val bitSetCapacity = 100

  def createRevision(
      revisionId: Long,
      pageId: Int,
      parentId: Long,
      timestamp: Long,
      sources: Seq[String] = Seq.empty
  ): Revision = {
    new Revision(
      revisionId = revisionId,
      pageId = pageId,
      parentId = parentId,
      timestamp = timestamp,
      templatePresence = new BitSet(bitSetCapacity),
      templateAdded = new BitSet(bitSetCapacity),
      templateRemoved = new BitSet(bitSetCapacity),
      templatePresenceGT = new BitSet(bitSetCapacity),
      templateAddedGT = new BitSet(bitSetCapacity),
      templateRemovedGT = new BitSet(bitSetCapacity),
      sources = sources
    )
  }
}
