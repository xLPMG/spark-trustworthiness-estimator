package me.lpmg.ste.data

object LinkResolver {

  /** Resolve page titles to page IDs in a revision.
    *
    * @param revision
    *   the revision to resolve page titles to page IDs
    * @param dictionary
    *   the dictionary map containing "Page Title -> Page ID"
    * @return
    *   the revision with resolved page titles to page IDs
    */
  def resolvePageTitlesToPageIDs(
      revision: Revision,
      dictionary: Map[String, Seq[String]]
  ): Revision = {
    // TODO: Resolve redirects
    val resolvedPageLinks = revision.outlinks.flatMap { pageTitle =>
      dictionary.getOrElse(pageTitle, Seq.empty).headOption
    }
    revision.copy(outlinks = resolvedPageLinks)
  }

  def resolvePageIDsToRevisionIDs(
      revision: Revision,
      groupedRevisions: Map[String, Seq[Revision]]
  ): Revision = {

    val resolvedPageLinks = revision.outlinks.flatMap { pageId =>
      groupedRevisions
        .getOrElse(pageId, Seq.empty)
        .filter((rev: Revision) => rev.timestamp.isBefore(revision.timestamp))
        .lastOption
        .map(_.revisionId)
        .toSeq
    }
    revision.copy(outlinks = resolvedPageLinks)
  }
}
