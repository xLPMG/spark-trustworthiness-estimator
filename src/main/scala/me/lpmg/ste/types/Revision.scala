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
  * @param resolvedPageOutlinks
  *   the outlinks of the revision (page IDs)
  * @param resolvedRevisionOutlinks
  *   the outlinks of the revision (revision IDs)
  * @param isRedirect
  *   whether the revision is a redirect
  * @param templatePresence
  *   the presence of templates in the revision
  * @param templateAdded
  *   whether templates were added in the revision
  * @param templateRemoved
  *   whether templates were removed in the revision
  */
class Revision(
    val revisionId: Long,
    val pageId: Int,
    val parentId: Long,
    val timestamp: Long,
    val resolvedPageOutlinks: Set[Int],
    val resolvedRevisionOutlinks: Set[Long],
    val isRedirect: Boolean,
    val templatePresence: BitSet = new BitSet(TemplateBitPositions.size),
    val templateAdded: BitSet = new BitSet(TemplateBitPositions.size),
    val templateRemoved: BitSet = new BitSet(TemplateBitPositions.size)
) extends Serializable {

  /** Copy the revision with the specified values. All unspecified values are
    * copied from the original revision.
    *
    * @param revisionId
    * @param pageId
    * @param parentId
    * @param timestamp
    * @param resolvedPageOutlinks
    * @param resolvedRevisionOutlinks
    * @param isRedirect
    * @param templatePresence
    * @param templateAdded
    * @param templateRemoved
    * @return
    *   a new revision with the specified values
    */
  def copy(
      revisionId: Long = this.revisionId,
      pageId: Int = this.pageId,
      parentId: Long = this.parentId,
      timestamp: Long = this.timestamp,
      resolvedPageOutlinks: Set[Int] = this.resolvedPageOutlinks,
      resolvedRevisionOutlinks: Set[Long] = this.resolvedRevisionOutlinks,
      isRedirect: Boolean = this.isRedirect,
      templatePresence: BitSet = this.templatePresence,
      templateAdded: BitSet = this.templateAdded,
      templateRemoved: BitSet = this.templateRemoved
  ): Revision = {
    new Revision(
      revisionId,
      pageId,
      parentId,
      timestamp,
      resolvedPageOutlinks,
      resolvedRevisionOutlinks,
      isRedirect,
      templatePresence,
      templateAdded,
      templateRemoved
    )
  }

  def toIdTimestampPair: (Long, Long) = (revisionId, timestamp)

  /** Convert the revision to a revision vertex.
    *
    * @return
    *   the revision vertex
    */
  def toRevisionVertex =
    new RevisionVertex(
      0.0f,
      isRedirect,
      templatePresence,
      templateAdded,
      templateRemoved
    )

  override def toString(): String = {
    s"Revision(revisionId=${revisionId}, pageId=${pageId}, parentId=${parentId}, timestamp=${timestamp}, isRedirect=${isRedirect}, templatePresence=${bitSetToBinaryString(templatePresence)}, templateAdded=${bitSetToBinaryString(templateAdded)}, templateRemoved=${bitSetToBinaryString(templateRemoved)})"
  }

  private def bitSetToBinaryString(bitSet: BitSet): String = {
    val binaryString = (0 until bitSet.capacity).map { bit =>
      if (bitSet.get(bit)) '1' else '0'
    }.mkString
    binaryString.reverse.dropWhile(_ == '0').reverse
  }
}
