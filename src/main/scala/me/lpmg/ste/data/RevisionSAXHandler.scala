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

  private var currentElement: String = ""
  private var insidePage = false
  private var insideRevision = false
  private var isMainNamespace = false

  private var pageId: Long = 0
  private var currentPageTitle: Option[String] = None
  private var revisionId: Long = 0
  private var parentId: Option[Long] = None
  private var timestamp: Long = 0

  private var outlinkPageIds: Set[Long] = Set()
  private var isRedirect: Boolean = false

  private val charBuffer = new StringBuilder

  override def startElement(
      uri: String,
      localName: String,
      qName: String,
      attributes: Attributes
  ): Unit = {
    currentElement = qName
    charBuffer.clear()

    qName match {
      case "page" =>
        insidePage = true
        currentPageTitle = None
        isMainNamespace = false // reset
      case "revision" =>
        insideRevision = true
        revisionId = 0
        parentId = None
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
      currentElement match {
        case "id" if revisionId == 0 =>
          revisionId = charBuffer.toString.trim.toLong
        case "parentid" =>
          parentId = Some(charBuffer.toString.trim.toLong)
        case "timestamp" =>
          try {
            timestamp = Instant.parse(charBuffer.toString.trim).toEpochMilli()
          } catch {
            case e: Exception =>
              println(
                s"Error parsing timestamp: ${charBuffer.toString.trim} for revision: $revisionId in page: $pageId"
              )
          }
        case "text" =>
          val content = charBuffer.toString.trim
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
            }.toSet
            outlinkPageIds =
              LinkResolver.resolvePageTitlesToPageIDs(pageTitles, dictionary)
          } else if (currentPageTitle.isDefined) {
            // for redirects, we resolve the redirect target and store it as the only outlink
            val redirectTarget =
              LinkResolver.resolveRedirect(currentPageTitle.get, dictionary)
            if (redirectTarget != -1) {
              outlinkPageIds = Set(redirectTarget)
            }
          }
        case _ => // No-op for other elements
      }
    } else if (insidePage && !insideRevision) {
      currentElement match {
        case "title" => currentPageTitle = Some(charBuffer.toString.trim)
        case _       => // No-op for other elements
      }
    }

    qName match {
      case "page" =>
        insidePage = false
      case "revision" =>
        if (insidePage && isMainNamespace) {
          // Add the extracted revision data to the list of revisions
          revisions += new Revision(
            revisionId,
            pageId,
            parentId,
            timestamp,
            false,
            0.0,
            outlinkPageIds,
            Set.empty,
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
    charBuffer.appendAll(ch, start, length)

    if (
      insidePage && currentElement == "ns" && charBuffer.toString.trim == "0"
    ) {
      isMainNamespace = true
    }

    if (insidePage && !insideRevision && currentElement == "id") {
      pageId =
        charBuffer.toString.trim.toLong // Page ID only needs to be set once per page
    }
  }

  def setDictionary(dictionary: Map[String, (Long, String)]): Unit = {
    this.dictionary = dictionary
  }
  def getRevisions: Seq[Revision] = revisions.toSeq
}
