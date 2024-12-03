package me.lpmg.ste.data

import org.apache.spark.input.PortableDataStream
import java.io.InputStream
import java.io.FileInputStream
import java.time.Instant
import me.lpmg.ste.types.Types.DictType
import me.lpmg.ste.types.Types.TemplateBitPositions

class DataReaderTest extends munit.FunSuite {
  test("testReadData") {
    val filePath = "src/test/resources/dump/test-dump-1.xml"
    val inputStream: InputStream = new FileInputStream(filePath)
    val revisions = DataReader.getRevisions(inputStream)
    // only 5 out of 7 revisions are in the main namespace
    assertEquals(revisions.length, 5)

    // compare the extracted revision data
    val unreferencedPosition: Int =
      TemplateBitPositions.get("Unreferenced").get.toInt
    revisions.foreach { revision =>
      if (revision.revisionId == 1L) {
        assertEquals(revision.pageId, 1)
        assertEquals(revision.parentId, -1L)
        assertEquals(
          revision.timestamp,
          Instant.parse("2011-01-01T00:00:01Z").toEpochMilli()
        )
        assertEquals(revision.templatePresence.get(unreferencedPosition), false)
      } else if (revision.revisionId == 2L) {
        assertEquals(revision.pageId, 2)
        assertEquals(revision.parentId, -1L)
        assertEquals(
          revision.timestamp,
          Instant.parse("2011-01-02T00:00:01Z").toEpochMilli()
        )
        assertEquals(revision.templatePresence.get(unreferencedPosition), false)
      } else if (revision.revisionId == 3L) {
        assertEquals(revision.pageId, 1)
        assertEquals(revision.parentId, 1L)
        assertEquals(
          revision.timestamp,
          Instant.parse("2011-01-03T00:00:01Z").toEpochMilli()
        )
        assertEquals(revision.templatePresence.get(unreferencedPosition), true)
      } else if (revision.revisionId == 4L) {
        assertEquals(revision.pageId, 2)
        assertEquals(revision.parentId, 2L)
        assertEquals(
          revision.timestamp,
          Instant.parse("2011-01-04T00:00:01Z").toEpochMilli()
        )
        assertEquals(revision.templatePresence.get(unreferencedPosition), false)
      }
    }
  }
}
