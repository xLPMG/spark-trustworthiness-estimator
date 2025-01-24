package me.lpmg.ste.data

import org.apache.spark.input.PortableDataStream
import java.io.InputStream
import java.io.FileInputStream
import java.time.Instant
import me.lpmg.ste.types.Types.TemplateBitPositions

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

    print(sources)

    // References
    assert(sources.contains("cbc.ca"))
    assert(sources.contains("whyfiles.org"))
    assert(sources.contains("ISBN:0684182413"))
    assert(sources.contains("mdw.ac.at"))
    assert(sources.contains("antiquity.ac.uk"))

    // External Links
    assert(sources.contains("fluteinfo.com"))
    assert(sources.contains("loc.gov"))
    assert(sources.contains("flutehistory.com"))
    assert(sources.contains("flutes.tk"))
    assert(sources.contains("thegalwaynetwork.com"))
    assert(sources.contains("larrykrantz.com"))
    assert(sources.contains("telus.net"))
    assert(sources.contains("johnmcmurtery.com"))
    assert(sources.contains("neyneva.com"))
    assert(sources.contains("woodwind.org"))
    assert(sources.contains("webindia123.com"))
    assert(sources.contains("ronkorb.com"))
    assert(sources.contains("bansuriflute.com"))
    assert(sources.contains("pnoyandthecity.blogspot.com"))
    assert(sources.contains("rastko.net"))
  }
}
