package me.lpmg.ste.data

import java.time.Instant

class LinkResolverTest extends munit.FunSuite {

  test("resolvePageIDsToRevisionIDs") {
    // Revision for which we want to resolve page IDs to revision IDs
    val revisionWithLink = new Revision(
      1L,
      1L,
      None,
      Instant.parse("2011-01-03T00:00:01Z").toEpochMilli(),
      false,
      0.0,
      Set(2L),
      Set.empty,
      false
    )

    // Grouped revisions by page ID
    val firstRevision = new Revision(
      2L,
      2L,
      None,
      Instant.parse("2011-01-01T00:00:01Z").toEpochMilli(),
      false,
      0.0,
      Set.empty,
      Set.empty,
      false
    )
    val secondRevision = new Revision(
      3L,
      2L,
      Some(2L),
      Instant.parse("2011-01-02T00:00:01Z").toEpochMilli(),
      false,
      0.0,
      Set.empty,
      Set.empty,
      false
    )
    // this revision is not before the revisionWithLink timestamp
    val thirdRevision = new Revision(
      4L,
      2L,
      Some(3L),
      Instant.parse("2011-01-04T00:00:01Z").toEpochMilli(),
      false,
      0.0,
      Set.empty,
      Set.empty,
      false
    )

    // group revisions by page ID
    val groupedRevisions = Map(
      1L -> Seq(revisionWithLink),
      2L -> Seq(firstRevision, secondRevision, thirdRevision)
    )

    val resolvedRevision =
      LinkResolver.resolvePageIDsToRevisionIDs(
        revisionWithLink,
        groupedRevisions
      )

    assertEquals(resolvedRevision.resolvedRevisionOutlinks, Set(3L))

  }

}
