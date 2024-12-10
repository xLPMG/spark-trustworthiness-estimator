package me.lpmg.ste.types

import org.apache.spark.util.collection.BitSet
import me.lpmg.ste.types.Types.TemplateBitPositions

/** A class to represent a Wikipedia revision.
  *
  * @param revisionId
  *   the unique identifier of the revision
  * @param pageId
  *   the unique identifier of the page
  * @param parentId
  *   the unique identifier of the parent revision
  * @param timestamp
  *   the timestamp of the revision
  * @param contributorId
  *   the unique identifier of the contributor
  * @param templatePresence
  *   the presence of templates in the revision
  * @param templateAdded
  *   whether templates were added in the revision
  * @param templateRemoved
  *   whether templates were removed in the revision
  * @param sources
  *   the sources of the revision
  */
class Revision(
    val revisionId: Long,
    val pageId: Int,
    val parentId: Long,
    val timestamp: Long,
    val contributorId: Int,
    val templatePresence: BitSet = new BitSet(TemplateBitPositions.size),
    val templateAdded: BitSet = new BitSet(TemplateBitPositions.size),
    val templateRemoved: BitSet = new BitSet(TemplateBitPositions.size),
    val sources: Seq[String] = Seq.empty
) extends Serializable {

  /** Copy the revision with the specified values. All unspecified values are
    * copied from the original revision.
    *
    * @param revisionId
    *   the unique identifier of the revision
    * @param pageId
    *   the unique identifier of the page
    * @param parentId
    *   the unique identifier of the parent revision
    * @param timestamp
    *   the timestamp of the revision
    * @param contributorId
    *   the unique identifier of the contributor
    * @param templatePresence
    *   the presence of templates in the revision
    * @param templateAdded
    *   whether templates were added in the revision
    * @param templateRemoved
    *   whether templates were removed in the revision
    * @param sources
    *   the sources of the revision
    * @return
    *   a new revision with the specified values
    */
  def copy(
      revisionId: Long = this.revisionId,
      pageId: Int = this.pageId,
      parentId: Long = this.parentId,
      timestamp: Long = this.timestamp,
      contributorId: Int = this.contributorId,
      templatePresence: BitSet = this.templatePresence,
      templateAdded: BitSet = this.templateAdded,
      templateRemoved: BitSet = this.templateRemoved,
      sources: Seq[String] = this.sources
  ): Revision = {
    new Revision(
      revisionId,
      pageId,
      parentId,
      timestamp,
      contributorId,
      templatePresence,
      templateAdded,
      templateRemoved,
      sources
    )
  }

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
  def toRevisionVertex =
    new RevisionVertex(
      0.0f,
      contributorId,
      templatePresence,
      templateAdded,
      templateRemoved,
      templateAdded.cardinality() > 0 || templateRemoved.cardinality() > 0
    )

  override def toString(): String = {
    s"Revision(revisionId=${revisionId}, pageId=${pageId}, parentId=${parentId}, timestamp=${timestamp}, contributorId=${contributorId}, templatePresence=${bitSetToBinaryString(templatePresence)}, templateAdded=${bitSetToBinaryString(
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
