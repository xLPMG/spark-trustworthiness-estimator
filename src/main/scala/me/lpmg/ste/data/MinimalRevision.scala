package me.lpmg.ste.data

/** A class to represent minimal information about a Wikipedia revision.
  *
  * @param revisionId
  *   the unique identifier of the revision
  * @param timestamp
  *   the timestamp of the revision
  */
class MinimalRevision(
    var revisionId: Long,
    var timestamp: Long
) extends Serializable
