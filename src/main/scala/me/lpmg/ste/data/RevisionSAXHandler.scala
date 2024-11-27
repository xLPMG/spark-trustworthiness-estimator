package me.lpmg.ste.data

import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import org.xml.sax.{Attributes, InputSource}
import java.time.Instant
import me.lpmg.ste.types.Types.{DictType, TemplateBitPositions}
import me.lpmg.ste.types.Revision
import org.apache.spark.util.collection.BitSet

/** SAX handler for parsing MediaWiki XML revisions.
  */
class RevisionSAXHandler(dateLimit: Long = 0) extends DefaultHandler {
  private val revisions = ArrayBuffer[Revision]()
  private val outlinkPattern = "\\[\\[([^\\]]+)\\]\\]".r

  private var dictionary: DictType = Map.empty

  private var insidePage = false
  private var insideRevision = false
  private var isMainNamespace = false

  private var pageTitle: String = ""
  private var pageId: Int = 0
  private var revisionId: Long = 0
  private var parentId: Option[Long] = None
  private var timestamp: Long = 0

  private var outlinkPageIds: Set[Int] = Set.empty
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
        outlinkPageIds = Set.empty[Int]
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
        case "id" => pageId = getBuffer.toInt
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

          // set template bits
          //TODO: check for things like {{Unreferenced|date=March 2019}}
          // TemplateBitPositions.foreach(
          //   template =>
          //     if (content.contains("{{" + template._1 + "}}") ||
          //         content.contains("{{" + template._1.toLowerCase + "}}")) {
          //       templateBitset.set(template._2)
          //     }
          // )
          
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
          if (timestamp > dateLimit) {
            revisions += new Revision(
              revisionId,
              pageId,
              parentId.getOrElse(-1),
              timestamp,
              outlinkPageIds,
              Set.empty,
              isRedirect,
            )
          }
        }
        insideRevision = false
      case _ => // No-op for other elements
    }
  }

  private def getBuffer: String = charBuffer.toString.trim

  def setDictionary(dictionary: DictType): Unit = {
    this.dictionary = dictionary
  }
  def getRevisions: Seq[Revision] = revisions
}
