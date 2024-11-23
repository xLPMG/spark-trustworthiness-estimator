package me.lpmg.ste.data

object LinkResolver {

  /** Resolve page titles to page IDs.
    *
    * @param titles
    *   set of page titles
    * @param dictionary
    *   dictionary mapping page titles to page IDs
    * @return
    */
  def resolvePageTitlesToPageIDs(
      titles: Set[String],
      dictionary: Map[String, (Long, String)]
  ): Set[Long] = {
    titles.flatMap { pageTitle =>
      dictionary.get(pageTitle).map(_._1)
    }
  }

  /** Resolve page IDs to revision IDs in a revision.
    *
    * @param revision
    *   the revision to resolve page IDs to revision IDs for
    * @param groupedRevisions
    * @param minimize
    *   whether to minimize the revision after resolving (deleting unnecessary
    *   data)
    * @return
    *   the edited revision
    */
  def resolvePageIDsToRevisionIDs(
      revision: Revision,
      groupedRevisions: Map[Long, Seq[MinimalRevision]],
      minimize: Boolean = true
  ): Revision = {

    revision.resolvedRevisionOutlinks = revision.resolvedPageOutlinks.flatMap {
      pageId =>
        groupedRevisions
          .getOrElse(pageId, Seq.empty)
          .filter((rev: MinimalRevision) => rev.timestamp < revision.timestamp)
          .lastOption
          .map(_.revisionId)
          .toSeq
    }
    val newPageOutlinks =
      if (minimize) Set.empty[Long] else revision.resolvedPageOutlinks

    new Revision(
      revision.revisionId,
      revision.pageId,
      revision.parentId,
      revision.timestamp,
      revision.isGroundTruth,
      revision.trustScore,
      newPageOutlinks,
      revision.resolvedRevisionOutlinks,
      revision.isRedirect
    )

  }
}
