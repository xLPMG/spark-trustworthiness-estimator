package me.lpmg.ste.data

import org.apache.spark.input.PortableDataStream
import java.io.InputStream
import java.io.FileInputStream

class DataReaderTest extends munit.FunSuite {
  test("testReadData") {
    val filePath = "src/test/resources/dump/test-dump-1.xml"
    val inputStream: InputStream = new FileInputStream(filePath)
    val revisions = DataReader.getRevisions(inputStream, "circular")
    // only 5 out of 7 revisions are in the main namespace
    assertEquals(revisions.length, 4)
    var revisionsChecked = 0
    // compare the extracted revision data
    revisions.foreach { revision =>
      if (revision.revisionId == 2L) {
        assertEquals(revision.pageId, 1)
        assertEquals(revision.templateAdded, true)
        assertEquals(revision.templateRemoved, false)
        assertEquals(revision.pairId, 3L)
        revisionsChecked += 1
      } else if (revision.revisionId == 3L) {
        assertEquals(revision.pageId, 1)
        assertEquals(revision.templateAdded, false)
        assertEquals(revision.templateRemoved, true)
        assertEquals(revision.pairId, 2L)
        revisionsChecked += 1

      } else if (revision.revisionId == 4L) {
        assertEquals(revision.pageId, 1)
        assertEquals(revision.templateAdded, true)
        assertEquals(revision.templateRemoved, false)
        assertEquals(revision.pairId, 6L)
        revisionsChecked += 1
      } else if (revision.revisionId == 5L) {
        assert(false, "Revision 5 should not exist")
      } else if (revision.revisionId == 6L) {
        assertEquals(revision.pageId, 1)
        assertEquals(revision.templateAdded, false)
        assertEquals(revision.templateRemoved, true)
        assertEquals(revision.pairId, 4L)
        revisionsChecked += 1
      }
    }

    assertEquals(revisionsChecked, 4)
  }

  test("extractSources") {
    val filePath = "src/test/resources/dump/test-dump-2.xml"
    val inputStream: InputStream = new FileInputStream(filePath)
    val revisions = DataReader.getRevisions(inputStream, "circular")
    val revision = revisions.head
    val sources = revision.sources

    //sources: lugnet.com, youtube.com, cbc.ca, whyfiles.org, ISBN:0684182413, mdw.ac.at, unsw.edu.au, antiquity.ac.uk
    assert(sources.contains("lugnet.com"))
    assert(sources.contains("youtube.com"))
    assert(sources.contains("cbc.ca"))
    assert(sources.contains("whyfiles.org"))
    assert(sources.contains("ISBN:0684182413"))
    assert(sources.contains("mdw.ac.at"))
    assert(sources.contains("unsw.edu.au"))
    assert(sources.contains("antiquity.ac.uk"))
  }

  test("extractInlineSources") {
    val filePath = "src/test/resources/dump/test-dump-3.xml"
    val inputStream: InputStream = new FileInputStream(filePath)
    val revisions = DataReader.getRevisions(inputStream, "Unreliable source?", inline = true)
    val revision = revisions.head
    val sources = revision.sources

    print(sources)
  }
}
