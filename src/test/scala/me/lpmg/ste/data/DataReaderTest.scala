package me.lpmg.ste.data

import org.apache.spark.input.PortableDataStream
import java.io.InputStream
import java.io.FileInputStream
import java.time.Instant

class DataReaderTest extends munit.FunSuite {
  test("testReadData") {
    val filePath = "src/test/resources/dump/test-dump-1.xml"
    val inputStream: InputStream = new FileInputStream(filePath)
    var dictionary: Map[String, (Long, String)] = Map("Page 2" -> (2, ""))
    val revisions = DataReader.getRevisions(inputStream, dictionary)
    // only 5 out of 7 revisions are in the main namespace
    assertEquals(revisions.length, 5)

    // compare the extracted revision data
    revisions.foreach { revision =>
      if (revision.revisionId == 1L) {
        assertEquals(revision.pageId, 1L)
        assertEquals(revision.parentId, None)
        assertEquals(revision.timestamp, Instant.parse("2011-01-01T00:00:01Z").toEpochMilli())
      } else if (revision.revisionId == 2L) {
        assertEquals(revision.pageId, 2L)
        assertEquals(revision.parentId, None)
        assertEquals(revision.timestamp, Instant.parse("2011-01-02T00:00:01Z").toEpochMilli())
      } else if (revision.revisionId == 3L) {
        assertEquals(revision.pageId, 1L)
        assertEquals(revision.parentId, Some(1L))
        assertEquals(revision.timestamp, Instant.parse("2011-01-03T00:00:01Z").toEpochMilli())
        // Revision 2 has link "[[Page 2]]"
        assertEquals(revision.resolvedPageOutlinks, Set(2L))
      } else if (revision.revisionId == 4L) {
        assertEquals(revision.pageId, 2L)
        assertEquals(revision.parentId, Some(2L))
        assertEquals(revision.timestamp, Instant.parse("2011-01-04T00:00:01Z").toEpochMilli())
      }
    }
  }

  test("getDictionary") {
    val filePath = "src/test/resources/dump/test-dump-1.xml"
    val inputStream: InputStream = new FileInputStream(filePath)
    val dictionary = DataReader.getDictionary(inputStream)
    inputStream.close()
    assert(dictionary.nonEmpty)
    assertEquals(dictionary.getOrElse("Page 1", ""), (1L, ""));
    assertEquals(dictionary.getOrElse("Page 2", ""), (2L, ""));
    assertEquals(dictionary.getOrElse("Page IGNORE", ""), (3L, ""));
    assertEquals(dictionary.getOrElse("page 1", ""), (11L, "Page 1"));
  }
}
