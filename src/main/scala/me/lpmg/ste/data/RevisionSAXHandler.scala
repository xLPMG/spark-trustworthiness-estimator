package me.lpmg.ste.data

import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import org.xml.sax.{Attributes, InputSource}
import java.time.Instant
import me.lpmg.ste.types.Types.{TemplateBitPositions}
import org.apache.spark.util.collection.BitSet

/** SAX handler for parsing MediaWiki XML revisions, filtering for specific
  * template and namespace.
  *
  * @param template
  *   The template to search for within page revisions
  */
class RevisionSAXHandler(template: String) extends DefaultHandler {
  private val revisions = ArrayBuffer[Revision]()

  // State tracking variables
  private var insidePage = false
  private var insideRevision = false
  private var insideContributor = false
  private var isMainNamespace = false
  private var isRedirect = false

  // Current page and revision context
  private var pageId: Int = 0
  private var currentRevisionId: Long = 0
  private var currentText: String = ""

  // Template tracking
  private var firstTemplateRevision: Option[Revision] = None
  private var lastTemplateRevision: Option[Revision] = None
  private var currentTemplatePresent: Boolean = false

  // Buffering for parsing
  private val charBuffer = new StringBuilder

  override def startElement(
      uri: String,
      localName: String,
      qName: String,
      attributes: Attributes
  ): Unit = {
    charBuffer.clear()

    qName match {
      case "revision" =>
        insideRevision = true
        currentTemplatePresent = false

      case "contributor" =>
        insideContributor = true

      case "page" =>
        insidePage = true
        insideContributor = false
        isMainNamespace = false
        isRedirect = false
        pageId = 0
        firstTemplateRevision = None
        lastTemplateRevision = None

      case "redirect" =>
        isRedirect = true

      case _ =>
      // Do nothing for other elements
    }
  }

  override def endElement(
      uri: String,
      localName: String,
      qName: String
  ): Unit = {
    val bufferContent = charBuffer.toString.trim
    qName match {
      // REVISION RELATED
      case "revision" if insidePage && !isRedirect && isMainNamespace =>
        // Process revision
        val templateCheck = checkTemplatePresence(bufferContent)

        if (templateCheck) {
          // First revision with template
          if (firstTemplateRevision.isEmpty) {
            if (pageId == 0) {
              print("Revision without page: ")
              print(currentRevisionId)
              print(" ")
              print(firstTemplateRevision.get.revisionId)
              println()
            }
            firstTemplateRevision = Some(
              Revision(
                revisionId = currentRevisionId,
                pairId = 0L, // Will be set later
                pageId = pageId,
                templateAdded = true,
                templateRemoved = false,
                templateAddedGT = true,
                templateRemovedGT = false,
                sources = SourceExtractor.extractSources(bufferContent)
              )
            )
          }
          currentTemplatePresent = true
        } else if (firstTemplateRevision.isDefined && !currentTemplatePresent) {
          if (pageId == 0) {
            print("Paired revision without page: ")
            print(currentRevisionId)
            print(" ")
            print(firstTemplateRevision.get.revisionId)
            println()
          }
          // Second revision without template
          lastTemplateRevision = Some(
            Revision(
              revisionId = currentRevisionId,
              pairId =
                firstTemplateRevision.get.revisionId, // Link to the first revision
              pageId = pageId,
              templateAdded = false,
              templateRemoved = true,
              templateAddedGT = false,
              templateRemovedGT = true,
              sources = SourceExtractor.extractSources(bufferContent)
            )
          )

          // Add the pair to revisions if both are present
          firstTemplateRevision.foreach(first =>
            lastTemplateRevision.foreach(last => {
              // Update firstTemplateRevision's pairId with last revision's revisionId
              val updatedFirst = first.copy(pairId = last.revisionId)
              revisions.append(updatedFirst)
              revisions.append(last)
            })
          )

          // Reset for next potential pair
          firstTemplateRevision = None
          lastTemplateRevision = None
        }

        insideRevision = false

      case "id" if insideRevision && !insideContributor =>
        currentRevisionId = bufferContent.toLong

      case "contributor" =>
        insideContributor = false

      case "text" if insideRevision =>
        currentText = bufferContent

      // PAGE RELATED
      case "page" =>
        // Reset page-level tracking
        insidePage = false

      case "ns" =>
        // Check if main namespace
        if (bufferContent == "0") {
          isMainNamespace = true
        }

      case "id" if insidePage && !insideRevision =>
        pageId = bufferContent.toInt

      case _ =>
      // Do nothing for other elements
    }
  }

  override def characters(
      ch: Array[Char],
      start: Int,
      length: Int
  ): Unit = {
    charBuffer.appendAll(ch, start, length)
  }

  /** Check if the template is present in the text (case-insensitive) */
  private def checkTemplatePresence(text: String): Boolean = {
    val lowercaseText = text.toLowerCase
    val lowercaseTemplate = template.toLowerCase
    lowercaseText.contains(s"{{$lowercaseTemplate}}") ||
    lowercaseText.contains(s"{{ $lowercaseTemplate }}") ||
    lowercaseText.contains(s"{{$lowercaseTemplate|")
  }

  /** Get the collected revisions */
  def getRevisions: Seq[Revision] = revisions
}
