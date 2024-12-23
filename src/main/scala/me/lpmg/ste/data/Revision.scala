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
      templateRemoved,
    )

  override def toString(): String = {
    s"Revision(revisionId=${revisionId}, pageId=${pageId}, parentId=${parentId}, timestamp=${timestamp}, templatePresence=${bitSetToBinaryString(templatePresence)}, templateAdded=${bitSetToBinaryString(
        templateAdded
      )}, templateRemoved=${bitSetToBinaryString(templateRemoved)}, sources=${sources.mkString(", ")})"
  }

  private def bitSetToBinaryString(bitSet: BitSet): String = {
    val binaryString = (0 until bitSet.capacity).map { bit =>
      if (bitSet.get(bit)) '1' else '0'
    }.mkString
    binaryString.reverse.dropWhile(_ == '0').reverse
  }
}
