package me.lpmg.ste.data

import me.lpmg.ste.types.Revision
import me.lpmg.ste.types.Types
import org.apache.spark.util.collection.BitSet

class TemplateUpdaterTest extends munit.FunSuite {
  test("updateTemplateBitSets") {

    val bitSetCapacity = Types.TemplateBitPositions.size
    val bitSetFirstBitPresent = new BitSet(bitSetCapacity)
    bitSetFirstBitPresent.set(0)
    val emptyBitSet = new BitSet(bitSetCapacity)

    // initialize revisions with empty bitsets
    var revision_1 = new Revision(
      revisionId = 1L,
      pageId = 1,
      parentId = -1L,
      contributorId = 1,
      timestamp = System.currentTimeMillis(),
      templatePresence = new BitSet(bitSetCapacity),
      templateAdded = new BitSet(bitSetCapacity),
      templateRemoved = new BitSet(bitSetCapacity)
    )

    var revision_2 = new Revision(
      revisionId = 2L,
      pageId = 1,
      parentId = 1L,
      timestamp = System.currentTimeMillis(),
      contributorId = 2,
      templatePresence = new BitSet(bitSetCapacity),
      templateAdded = new BitSet(bitSetCapacity),
      templateRemoved = new BitSet(bitSetCapacity)
    )

    var revision_3 = new Revision(
      revisionId = 3L,
      pageId = 1,
      parentId = 2L,
      timestamp = System.currentTimeMillis(),
      contributorId = 3,
      templatePresence = new BitSet(bitSetCapacity),
      templateAdded = new BitSet(bitSetCapacity),
      templateRemoved = new BitSet(bitSetCapacity)
    )

    // Test case: set a templace presence on revision_2
    revision_2 = revision_2.copy(templatePresence = bitSetFirstBitPresent)
    
    val revisions = Seq(revision_1, revision_2, revision_3)
    val revisionMap = revisions.map(revision => (revision.revisionId, revision.templatePresence)).toMap

    val updatedRevisions = TemplateUpdater.updateTemplateBitSets(revisions, revisionMap)

    updatedRevisions.foreach { revision =>
        if(revision.revisionId == 1L) {
            // revision_1 - untouched
            assertEquals(revision.templatePresence.equals(emptyBitSet), true)
            assertEquals(revision.templateAdded.equals(emptyBitSet), true)
            assertEquals(revision.templateRemoved.equals(emptyBitSet), true)
        } else if(revision.revisionId == 2L) {
            // revision_2 - presence was set, now added should be set
            assertEquals(revision.templatePresence.equals(bitSetFirstBitPresent), true)
            assertEquals(revision.templateAdded.equals(bitSetFirstBitPresent), true)
            assertEquals(revision.templateRemoved.equals(emptyBitSet), true)
        } else if(revision.revisionId == 3L) {
            // revision_3 - presence was not set, now removed should be set
            assertEquals(revision.templatePresence.equals(emptyBitSet), true)
            assertEquals(revision.templateAdded.equals(emptyBitSet), true)
            assertEquals(revision.templateRemoved.equals(bitSetFirstBitPresent), true)

        }
    }
  }
}
