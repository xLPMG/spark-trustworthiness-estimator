package me.lpmg.ste.data

/** A class to represent a Wikipedia revision.
  *
  *  @constructor create a new revision with provided revisionId, pageId, parentId, and timestamp
  *  @param revisionId the unique identifier of the revision
  *  @param pageId the unique identifier of the page
  *  @param parentId the unique identifier of the parent revision
  *  @param timestamp the timestamp of the revision
  */
final case class Revision(
  revisionId: String,
  pageId: String,
  parentId: Option[String],
  timestamp: String
)