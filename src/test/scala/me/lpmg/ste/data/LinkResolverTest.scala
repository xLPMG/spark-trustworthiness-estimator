package me.lpmg.ste.data

import java.time.Instant

class LinkResolverTest extends munit.FunSuite {

  test("resolvePageTitlesToPageIDs") {
    val revision = Revision(
      "1",
      "1",
      None,
      Instant.parse("2011-01-01T00:00:01Z"),
      false,
      0.0,
      Set("Page 1", "Page 2"),
      false
    )
    val dictionary = Map(
      "Page 1" -> Seq("1", "Page 1"),
      "Page 2" -> Seq("2", "Page 2")
    )
    val resolvedRevision =
      LinkResolver.resolvePageTitlesToPageIDs(revision, dictionary)
    assertEquals(resolvedRevision.outlinks, Set("1", "2"))
  }

  test("resolvePageIDsToRevisionIDs") {
    // Revision for which we want to resolve page IDs to revision IDs
    val revisionWithLink = Revision(
      "1",
      "1",
      None,
      Instant.parse("2011-01-03T00:00:01Z"),
      false,
      0.0,
      Set("2"),
      false
    )

    // Grouped revisions by page ID
    val firstRevision = Revision(
      "2",
      "2",
      None,
      Instant.parse("2011-01-01T00:00:01Z"),
      false,
      0.0,
      Set.empty,
      false
    )
    val secondRevision = Revision(
      "3",
      "2",
      Some("2"),
      Instant.parse("2011-01-02T00:00:01Z"),
      false,
      0.0,
      Set.empty,
      false
    )
    // this revision is not before the revisionWithLink timestamp
    val thirdRevision = Revision(
      "4",
      "2",
      Some("3"),
      Instant.parse("2011-01-04T00:00:01Z"),
      false,
      0.0,
      Set.empty,
      false
    )

    // group revisions by page ID
    val groupedRevisions = Map(
      "1" -> Seq(revisionWithLink),
      "2" -> Seq(firstRevision, secondRevision, thirdRevision)
    )

    val resolvedRevision =
      LinkResolver.resolvePageIDsToRevisionIDs(
        revisionWithLink,
        groupedRevisions
      )

    assertEquals(resolvedRevision.outlinks, Set("3"))

  }

}
