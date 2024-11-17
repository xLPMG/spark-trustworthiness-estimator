package me.lpmg.ste.data

import org.apache.spark.input.PortableDataStream
import java.io.InputStream
import java.io.FileInputStream
import java.time.Instant

class DataReaderTest extends munit.FunSuite {
  test("testReadData") {
    val filePath = "src/test/resources/dump/test-dump-1.xml"
    val inputStream: InputStream = new FileInputStream(filePath)
    val revisions = DataReader.getRevisions(inputStream)
    // only 5 out of 7 revisions are in the main namespace
    assertEquals(revisions.length, 5)

    // compare the extracted revision data
    revisions.foreach { revision =>
      if (revision.revisionId == "1") {
        assertEquals(revision.pageId, "1")
        assertEquals(revision.parentId, None)
        assertEquals(revision.timestamp, Instant.parse("2011-01-01T00:00:01Z"))
      } else if (revision.revisionId == "2") {
        assertEquals(revision.pageId, "2")
        assertEquals(revision.parentId, None)
        assertEquals(revision.timestamp, Instant.parse("2011-01-02T00:00:01Z"))
      } else if (revision.revisionId == "3") {
        assertEquals(revision.pageId, "1")
        assertEquals(revision.parentId, Some("1"))
        assertEquals(revision.timestamp, Instant.parse("2011-01-03T00:00:01Z"))
      } else if (revision.revisionId == "4") {
        assertEquals(revision.pageId, "2")
        assertEquals(revision.parentId, Some("2"))
        assertEquals(revision.timestamp, Instant.parse("2011-01-04T00:00:01Z"))
      }
    }
  }

  test("getDictionary") {
    val filePath = "src/test/resources/dump/test-dump-1.xml"
    val inputStream: InputStream = new FileInputStream(filePath)
    val dictionary = DataReader.getDictionary(inputStream)
    inputStream.close()
    assert(dictionary.nonEmpty)
    assertEquals(dictionary.getOrElse("Page 1", ""), Seq("1", ""));
    assertEquals(dictionary.getOrElse("Page 2", ""), Seq("2", ""));
    assertEquals(dictionary.getOrElse("Page IGNORE", ""), Seq("3", ""));
    assertEquals(dictionary.getOrElse("page 1", ""), Seq("11", "Page 1"));
  }
}
