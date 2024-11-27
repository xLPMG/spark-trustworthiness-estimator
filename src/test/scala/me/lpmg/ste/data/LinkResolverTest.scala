package me.lpmg.ste.data

import java.time.Instant
import me.lpmg.ste.types.Revision
import org.apache.spark.util.collection.BitSet

class LinkResolverTest extends munit.FunSuite {

  test("resolvePageIDsToRevisionIDs") {
    // Revision for which we want to resolve page IDs to revision IDs
    val revisionWithLink = new Revision(
      1L,
      1,
      -1L,
      Instant.parse("2011-01-03T00:00:01Z").toEpochMilli(),
      Set(2),
      Set.empty,
      false,
      null,
      null,
      null
    )

    // Grouped revisions by page ID
    val firstRevision = new Revision(
      2L,
      2,
      -1L,
      Instant.parse("2011-01-01T00:00:01Z").toEpochMilli(),
      Set.empty,
      Set.empty,
      false,
      null,
      null,
      null
    )
    
    val secondRevision = new Revision(
      3L,
      2,
      2L,
      Instant.parse("2011-01-02T00:00:01Z").toEpochMilli(),
      Set.empty,
      Set.empty,
      false,
      null,
      null,
      null
    )

    // this revision is not before the revisionWithLink timestamp
    val thirdRevision = new Revision(
      4L,
      2,
      3L,
      Instant.parse("2011-01-04T00:00:01Z").toEpochMilli(),
      Set.empty,
      Set.empty,
      false,
      null,
      null,
      null
    )

    // group revisions by page ID
    val groupedRevisions = Map(
      1 -> Seq(revisionWithLink.toIdTimestampPair),
      2 -> Seq(
        firstRevision.toIdTimestampPair,
        secondRevision.toIdTimestampPair,
        thirdRevision.toIdTimestampPair
      )
    )

    val resolvedRevision =
      LinkResolver.resolvePageIDsToRevisionIDs(
        revisionWithLink,
        groupedRevisions
      )

    assertEquals(resolvedRevision.resolvedRevisionOutlinks, Set(3L))

  }

  test("resolveRedirect") {
    val dictionary = Map(
      "ThisIsARedirect" -> (2, "RealPage"),
      "RealPage" -> (1, "")
    )

    val realPageId = LinkResolver.resolveRedirect("ThisIsARedirect", dictionary)
    assertEquals(realPageId, 1)
  }

  test("resolveRedirectToRedirect") {

    val dictionary = Map(
      "ThisIsARedirect" -> (3, "ThisIsARedirect2"),
      "ThisIsARedirect2" -> (2, "RealPage"),
      "RealPage" -> (1, "")
    )

    val realPageId = LinkResolver.resolveRedirect("ThisIsARedirect", dictionary)
    assertEquals(realPageId, 1)
  }

}
