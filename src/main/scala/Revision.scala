final case class Revision(
  revisionId: String,
  pageId: String,
  parentId: Option[String],
  timestamp: String
)