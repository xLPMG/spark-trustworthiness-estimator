package me.lpmg.ste.data

class DataReaderTest extends munit.FunSuite {
  test("testReadData") {
    val filePath = "src/test/resources/dump/test-dump-1.xml.bz2"
    val revisions = DataReader.parseXMLFile(filePath)
    // only 4 out of 6 revisions are in the main namespace
    assertEquals(revisions.length, 4)

    // compare the extracted revision data
    revisions.foreach { revision =>
        if (revision.revisionId == "1") {
            assertEquals(revision.pageId, "1")
            assertEquals(revision.parentId, None)
            assertEquals(revision.timestamp, "2011-01-01T00:00:01Z")
        }else if (revision.revisionId == "2") {
            assertEquals(revision.pageId, "2")
            assertEquals(revision.parentId, None)
            assertEquals(revision.timestamp, "2011-01-02T00:00:01Z")
        }else if (revision.revisionId == "3") {
            assertEquals(revision.pageId, "1")
            assertEquals(revision.parentId, Some("1"))
            assertEquals(revision.timestamp, "2011-01-03T00:00:01Z")
        }else if (revision.revisionId == "4") {
            assertEquals(revision.pageId, "2")
            assertEquals(revision.parentId, Some("2"))
            assertEquals(revision.timestamp, "2011-01-04T00:00:01Z")
        }
    }
  }
}
