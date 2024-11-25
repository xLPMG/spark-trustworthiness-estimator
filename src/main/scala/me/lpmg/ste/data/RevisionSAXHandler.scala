package me.lpmg.ste.data

import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import org.xml.sax.{Attributes, InputSource}
import java.time.Instant

/** SAX handler for parsing MediaWiki XML revisions.
  */
class RevisionSAXHandler extends DefaultHandler {
  private val revisions = ArrayBuffer[Revision]()
  private val outlinkPattern = "\\[\\[([^\\]]+)\\]\\]".r

  private var dictionary: Map[String, (Long, String)] = Map.empty

  private var insidePage = false
  private var insideRevision = false
  private var isMainNamespace = false

  private var pageTitle: String = ""
  private var pageId: Long = 0
  private var revisionId: Long = 0
  private var parentId: Option[Long] = None
  private var timestamp: Long = 0

  private var outlinkPageIds: Set[Long] = Set.empty
  private var isRedirect: Boolean = false

  private val charBuffer = new StringBuilder

  override def startElement(
      uri: String,
      localName: String,
      qName: String,
      attributes: Attributes
  ): Unit = {
    charBuffer.clear()

    qName match {
      case "page" =>
        insidePage = true
        isMainNamespace = false // reset
        pageTitle = ""
      case "revision" =>
        insideRevision = true
        revisionId = 0
        parentId = None
        timestamp = 0
        outlinkPageIds = Set.empty[Long]
        isRedirect = false
      case _ => // No-op for other tags
    }
  }

  override def endElement(
      uri: String,
      localName: String,
      qName: String
  ): Unit = {
    if (insideRevision) {
      // revision specific data
      handleRevision(qName)
    } else if (insidePage && !insideRevision) {
      // page specific data
      qName match {
        case "title" => pageTitle = getBuffer
        case "page"  => insidePage = false
        case "ns" =>
          if ("0".equals(getBuffer)) isMainNamespace = true
        case "id" => pageId = getBuffer.toLong
        case _    => // No-op for other elements
      }
    }
  }

  override def characters(
      ch: Array[Char],
      start: Int,
      length: Int
  ): Unit = {
    charBuffer.appendAll(ch, start, length)
  }

  /** Handles the revision elements in the XML.
    *
    * @param currentElement
    */
  private def handleRevision(currentElement: String): Unit = {
    currentElement match {
      case "id" if revisionId == 0 =>
        revisionId = getBuffer.toLong
      case "parentid" =>
        parentId = Some(getBuffer.toLong)
      case "timestamp" =>
        try {
          timestamp = Instant.parse(getBuffer).toEpochMilli()
        } catch {
          case e: Exception =>
            println(
              s"Error parsing timestamp: ${getBuffer} for revision: $revisionId in page: $pageId"
            )
        }
      case "text" =>
        val content = getBuffer
        isRedirect = content.startsWith("#REDIRECT")
        if (!isRedirect) {
          /* As soon as the links are found, they are resolved to page IDs.
           * While that might seem impractical, it was absolutely necessary
           * because storing all the strings required an extremely large
           * amount of memory.
           */
          val fullLinks = outlinkPattern
            .findAllMatchIn(content)
            .map(_.group(1))
            .filter(Filter.isArticleTitle)
            .toSet
          val pageTitles = fullLinks.flatMap { link =>
            val parts = link.split("\\|")
            if (parts.nonEmpty && parts.head.nonEmpty) Some(parts.head)
            else None
          }
          outlinkPageIds =
            LinkResolver.resolvePageTitlesToPageIDs(pageTitles, dictionary)
        } else if (pageTitle.length() > 0) {
          // for redirects, we resolve the redirect target and store it as the only outlink
          val redirectTarget =
            LinkResolver.resolveRedirect(pageTitle, dictionary)
          if (redirectTarget != -1) {
            outlinkPageIds = Set(redirectTarget)
          }
        }
      case "revision" =>
        // only add the revision if it is in the main namespace
        if (insidePage && isMainNamespace) {
          revisions += new Revision(
            revisionId,
            pageId,
            parentId.getOrElse(-1),
            timestamp,
            false,
            0.0,
            outlinkPageIds,
            Set.empty,
            isRedirect
          )
        }
        insideRevision = false
      case _ => // No-op for other elements
    }
  }

  private def getBuffer: String = charBuffer.toString.trim

  def setDictionary(dictionary: Map[String, (Long, String)]): Unit = {
    this.dictionary = dictionary
  }
  def getRevisions: Seq[Revision] = revisions
}
