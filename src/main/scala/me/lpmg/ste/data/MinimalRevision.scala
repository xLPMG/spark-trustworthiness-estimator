package me.lpmg.ste.data

/** Represents minimal information about a Wikipedia revision.
  *
  * @param revisionId
  *   the unique identifier of the revision
  * @param timestamp
  *   the timestamp of the revision
  */
final case class MinimalRevision(
    val revisionId: Long,
    val timestamp: Long
) extends Serializable
