package me.lpmg.ste.data

object LinkResolver {

  def resolvePageTitlesToPageIDs(
      titles: Set[String],
      dictionary: Map[String, Seq[String]]
  ): Set[Long] = {
    titles.flatMap { pageTitle =>
      dictionary.getOrElse(pageTitle, Seq.empty).headOption.map(_.toLong)
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
      groupedRevisions: Map[Long, Seq[Revision]],
      minimize: Boolean = true
  ): Revision = {

    revision.resolvedRevisionOutlinks = revision.resolvedPageOutlinks.flatMap {
      pageId =>
        groupedRevisions
          .getOrElse(pageId, Seq.empty)
          .filter((rev: Revision) => rev.timestamp < revision.timestamp)
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
