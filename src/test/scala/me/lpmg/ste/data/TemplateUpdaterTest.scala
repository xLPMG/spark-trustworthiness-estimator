package me.lpmg.ste.data

import org.junit.Test
import org.junit.Assert._
import me.lpmg.ste.types.Types.TemplateBitPositions
import me.lpmg.ste.rules.RevisionTestRule

class TemplateUpdaterTest extends munit.FunSuite {
  test("testUpdateTemplateBitSets") {
    val templateBitPosition: Int = 0

    val revision1 = RevisionTestRule.createRevision(
      revisionId = 1L,
      pageId = 1,
      parentId = -1L,
      timestamp = System.currentTimeMillis()
    )

    val revision2 = RevisionTestRule.createRevision(
      revisionId = 2L,
      pageId = 1,
      parentId = 1L,
      timestamp = System.currentTimeMillis()
    )

    val revision3 = RevisionTestRule.createRevision(
      revisionId = 3L,
      pageId = 1,
      parentId = 2L,
      timestamp = System.currentTimeMillis()
    )

    // Set template presence for revision 2
    revision2.templatePresence.set(templateBitPosition)

    val revisions = Seq(revision1, revision2, revision3)
    val revisionIdToTemplatesPresenceMap = revisions
      .map(revision => (revision.revisionId, revision.templatePresence))
      .toMap
    val updatedRevisions = TemplateUpdater.updateTemplateBitSets(
      revisions,
      revisionIdToTemplatesPresenceMap
    )

    // Check template changes
    updatedRevisions.foreach { revision =>
      if (revision.revisionId == 1L) {
        // revision_1 - untouched
        assertEquals(revision.templatePresence.get(templateBitPosition), false)
        assertEquals(revision.templateAdded.get(templateBitPosition), false)
        assertEquals(revision.templateRemoved.get(templateBitPosition), false)
      } else if (revision.revisionId == 2L) {
        // revision_2 - presence was set, now added should be set
        assertEquals(revision.templatePresence.get(templateBitPosition), true)
        assertEquals(revision.templateAdded.get(templateBitPosition), true)
        assertEquals(revision.templateRemoved.get(templateBitPosition), false)
      } else if (revision.revisionId == 3L) {
        // revision_3 - presence was not set, now removed should be set
        assertEquals(revision.templatePresence.get(templateBitPosition), false)
        assertEquals(revision.templateAdded.get(templateBitPosition), false)
        assertEquals(revision.templateRemoved.get(templateBitPosition), true)
      }
    }
  }
}
