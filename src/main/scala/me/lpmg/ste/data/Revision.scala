package me.lpmg.ste.data

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
  * @param isGroundTruth
  *   whether the revision is a ground truth revision
  * @param trustScore
  *   the trust score of the revision
  * @param resolvedPageOutlinks
  *   the outlinks of the revision (page IDs)
  * @param resolvedRevisionOutlinks
  *   the outlinks of the revision (revision IDs)
  * @param isRedirect
  *   whether the revision is a redirect
  */
class Revision(
    val revisionId: Long,
    val pageId: Long,
    val parentId: Long,
    val timestamp: Long,
    var isGroundTruth: Boolean,
    var trustScore: Double,
    var resolvedPageOutlinks: Set[Long],
    var resolvedRevisionOutlinks: Set[Long],
    val isRedirect: Boolean
) extends Serializable {
  def toMinimalRevision = new MinimalRevision(revisionId, timestamp)
}
