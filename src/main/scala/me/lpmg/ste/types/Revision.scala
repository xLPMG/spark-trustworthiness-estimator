package me.lpmg.ste.types

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
  */
class Revision(
    val revisionId: Long,
    val pageId: Int,
    val parentId: Long,
    val timestamp: Long,
    val resolvedPageOutlinks: Set[Int],
    val resolvedRevisionOutlinks: Set[Long],
    val isRedirect: Boolean,
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
  ): Revision = {
    new Revision(
      revisionId,
      pageId,
      parentId,
      timestamp,
      resolvedPageOutlinks,
      resolvedRevisionOutlinks,
      isRedirect,
    )
  }

  def toIdTimestampPair: (Long, Long) = (revisionId, timestamp)

  /** Convert the revision to a revision vertex.
    *
    * @return
    *   the revision vertex
    */
  def toRevisionVertex =
    new RevisionVertex(false, 0.0f, isRedirect)
}
