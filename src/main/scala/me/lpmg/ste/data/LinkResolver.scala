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
      dictionary: Types.DictType
  ): Set[Int] = {
    titles.flatMap { pageTitle =>
      dictionary.get(pageTitle).map(_._1)
    }
  }

  /** Gets the real page ID for a redirect page title. If the page title is not
    * a redirect, the function returns -1. If the real page is also a redirect,
    * it will resolve it recursively up to a depth of 2.
    *
    * @param pageTitle
    *   the page title to get the redirect ID for
    * @param dictionary
    *   dictionary mapping page titles to page IDs
    * @return
    *   the redirect ID if the page title is a redirect, -1 otherwise
    */
  def resolveRedirect(
      pageTitle: String,
      dictionary: Types.DictType
  ): Int = {
    def getRedirectIDHelper(title: String, depth: Int): Int = {
      if (depth > 2) -1
      else {
        dictionary.get(title) match {
          case Some((_, redirectTitle)) if redirectTitle.nonEmpty =>
            getRedirectIDHelper(redirectTitle, depth + 1)
          case Some((pageID, _)) => pageID
          case None              => -1
        }
      }
    }
    getRedirectIDHelper(pageTitle, 0)
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
      groupedRevisions: Map[Int, Seq[MinimalRevision]],
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
      if (minimize) Set.empty[Int] else revision.resolvedPageOutlinks

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
