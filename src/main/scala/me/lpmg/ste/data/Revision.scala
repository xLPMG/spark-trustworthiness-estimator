package me.lpmg.ste.data

import org.apache.spark.util.collection.BitSet
import me.lpmg.ste.types.Types.TemplateBitPositions
import me.lpmg.ste.graph

case class Revision(
    val revisionId: Long,
    val pageId: Int,
    val parentId: Long,
    val timestamp: Long,
    val templatePresence: BitSet = new BitSet(TemplateBitPositions.size),
    val templateAdded: BitSet = new BitSet(TemplateBitPositions.size),
    val templateRemoved: BitSet = new BitSet(TemplateBitPositions.size),
    val templatePresenceGT: BitSet = new BitSet(TemplateBitPositions.size),
    val templateAddedGT: BitSet = new BitSet(TemplateBitPositions.size),
    val templateRemovedGT: BitSet = new BitSet(TemplateBitPositions.size),
    val sources: Seq[String] = Seq.empty
) extends Serializable {

  /** Convert the revision to a pair of revision ID and timestamp.
    *
    * @return
    *   the pair of revision ID and timestamp
    */
  def toIdTimestampPair: (Long, Long) = (revisionId, timestamp)

  /** Convert the revision to a revision vertex.
    *
    * @return
    *   the revision vertex
    */
  def toRevisionVertex() =
    new graph.RevisionVertex(
      revisionId,
      0.0f,
      templatePresence,
      templateAdded,
      templateRemoved
    )

  def copy(
      revisionId: Long = this.revisionId,
      pageId: Int = this.pageId,
      parentId: Long = this.parentId,
      timestamp: Long = this.timestamp,
      templatePresence: BitSet = this.templatePresence,
      templateAdded: BitSet = this.templateAdded,
      templateRemoved: BitSet = this.templateRemoved,
      templatePresenceGT: BitSet = this.templatePresenceGT,
      templateAddedGT: BitSet = this.templateAddedGT,
      templateRemovedGT: BitSet = this.templateRemovedGT,
      sources: Seq[String] = this.sources
  ): Revision = {
    new Revision(
      revisionId,
      pageId,
      parentId,
      timestamp,
      cloneBitSet(templatePresence),
      cloneBitSet(templateAdded),
      cloneBitSet(templateRemoved),
      cloneBitSet(templatePresenceGT),
      cloneBitSet(templateAddedGT),
      cloneBitSet(templateRemovedGT),
      sources
    )
  }

  override def toString(): String = {
    s"Revision(revisionId=${revisionId}, pageId=${pageId}, parentId=${parentId}, timestamp=${timestamp}, templatePresence=${bitSetToBinaryString(templatePresence)}, templateAdded=${bitSetToBinaryString(
        templateAdded
      )}, templateRemoved=${bitSetToBinaryString(templateRemoved)}, templatePresenceGT=${bitSetToBinaryString(templatePresenceGT)}, templateAddedGT=${bitSetToBinaryString(
        templateAddedGT
      )}, templateRemovedGT=${bitSetToBinaryString(templateRemovedGT)}, sources=${sources.mkString(", ")})"
  }

  private def bitSetToBinaryString(bitSet: BitSet): String = {
    val binaryString = (0 until bitSet.capacity).map { bit =>
      if (bitSet.get(bit)) '1' else '0'
    }.mkString
    binaryString.reverse.dropWhile(_ == '0').reverse
  }

  def cloneBitSet(bitSet: BitSet): BitSet = {
    var newBitSet = new BitSet(bitSet.capacity)
    for (i <- 0 until bitSet.capacity) {
      if (bitSet.get(i)) {
        newBitSet.set(i)
      }
    }

    newBitSet
  }
}
