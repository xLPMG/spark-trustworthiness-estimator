package me.lpmg.ste.data

import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.ArrayBuffer
import org.xml.sax.{Attributes, InputSource}

/** SAX handler for parsing MediaWiki XML revisions.
  */
class RevisionSAXHandler extends DefaultHandler {
  private val revisions = ArrayBuffer[Revision]()
  private var currentElement: String = ""
  private var insidePage = false
  private var insideRevision = false
  private var isMainNamespace = false

  private var pageId: String = ""
  private var revisionId: String = ""
  private var parentId: Option[String] = None
  private var timestamp: String = ""

  private var isGroundTruth: Boolean = false
  private var trustScore: Double = 0.0
  private var outlinks: Set[String] = Set()
  private var isRedirect: Boolean = false

  override def startElement(
      uri: String,
      localName: String,
      qName: String,
      attributes: Attributes
  ): Unit = {
    currentElement = qName

    qName match {
      case "page" =>
        insidePage = true
        isMainNamespace = false // reset
      case "revision" =>
        insideRevision = true
        revisionId = ""
        parentId = None
        outlinks = Set()
        isRedirect = false
      case _ => // No-op for other tags
    }
  }

  override def endElement(
      uri: String,
      localName: String,
      qName: String
  ): Unit = {
    qName match {
      case "page" =>
        insidePage = false
      case "revision" =>
        if (insidePage && isMainNamespace) {
          // Add the extracted revision data to the list of revisions
          revisions += Revision(
            revisionId,
            pageId,
            parentId,
            timestamp,
            isGroundTruth,
            trustScore,
            outlinks,
            isRedirect
          )
        }
        insideRevision = false
      case _ => // No-op for other tags
    }
    currentElement = ""
  }

  override def characters(
      ch: Array[Char],
      start: Int,
      length: Int
  ): Unit = {
    val content = new String(ch, start, length).trim

    if (insidePage && currentElement == "ns" && content == "0") {
      isMainNamespace = true
    }

    if (insidePage && !insideRevision && currentElement == "id") {
      pageId = content // Page ID only needs to be set once per page
    }

    if (insideRevision) {
      currentElement match {
        case "id" if revisionId.isEmpty =>
          revisionId = content
        case "parentid" =>
          parentId = Some(content)
        case "timestamp" =>
          timestamp = content
        case "text" =>
          isRedirect = content.startsWith("#REDIRECT")
        // filter outlinks
        case _ => // No-op for other elements
      }
    }
  }

  def getRevisions: Seq[Revision] = revisions.toSeq
}
