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
  private var isMainNamespace = false
  private var isRedirect = false

  // Current page and revision context
  private var pageId: Option[Int] = None
  private var currentRevisionId: Option[Long] = None
  private var currentRevisionText: String = ""

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
      case "revision" if !isRedirect && isMainNamespace =>
        resetRevision()

      case "page" =>
        resetPage()

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
      case "revision" if !isRedirect && isMainNamespace =>
        // Process revision
        val templateCheck = checkTemplatePresence(currentRevisionText)

        if (templateCheck) {
          // First revision with template
          if (firstTemplateRevision.isEmpty) {
            firstTemplateRevision = Some(
              Revision(
                revisionId = currentRevisionId.getOrElse(-1L),
                pairId = -1L, // Will be set later
                pageId = pageId.getOrElse(-1),
                templateAdded = true,
                templateRemoved = false,
                templateAddedGT = true,
                templateRemovedGT = false,
                sources = SourceExtractor.extractSources(currentRevisionText)
              )
            )
            currentTemplatePresent = true
          } else {
            // Reset if template is found again without removal
            currentTemplatePresent = true
          }
        } else if (firstTemplateRevision.isDefined && !currentTemplatePresent) {
          // Revision without template after a template was added
          lastTemplateRevision = Some(
            Revision(
              revisionId = currentRevisionId.getOrElse(-1L),
              pairId = firstTemplateRevision
                .map(_.revisionId)
                .getOrElse(-1L), // Link to the first revision
              pageId = pageId.getOrElse(-1),
              templateAdded = false,
              templateRemoved = true,
              templateAddedGT = false,
              templateRemovedGT = true,
              sources = SourceExtractor.extractSources(currentRevisionText)
            )
          )

          // Add the pair to revisions if both are present and last revision has sources
          for {
            first <- firstTemplateRevision
            last <- lastTemplateRevision if last.sources.nonEmpty
          } {
            // Update firstTemplateRevision's pairId with last revision's revisionId
            val updatedFirst = first.copy(pairId = last.revisionId)
            revisions.append(updatedFirst)
            revisions.append(last)
          }

          // Reset for next potential pair
          firstTemplateRevision = None
          lastTemplateRevision = None
          currentTemplatePresent = false
        } else {
          // No template or no significant change
          currentTemplatePresent = templateCheck
        }

        insideRevision = false

      case "id" if insideRevision && !currentRevisionId.isDefined =>
        currentRevisionId = Some(bufferContent.toLong)

      case "text" if insideRevision =>
        currentRevisionText = bufferContent

      // PAGE RELATED
      case "page" =>
        // Reset page-level tracking
        insidePage = false

      case "ns" =>
        // Check if main namespace
        if (bufferContent == "0") {
          isMainNamespace = true
        }

      case "id" if insidePage && !insideRevision && !pageId.isDefined =>
        pageId = Some(bufferContent.toInt)

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
    lowercaseText.contains(s"{{$lowercaseTemplate|")
  }

  private def resetRevision(): Unit = {
    insideRevision = true
    currentRevisionId = None
    currentTemplatePresent = false
    currentRevisionText = ""
  }

  private def resetPage(): Unit = {
    insidePage = true
    isMainNamespace = false
    isRedirect = false
    pageId = None
    firstTemplateRevision = None
    lastTemplateRevision = None
  }

  /** Get the collected revisions */
  def getRevisions: Seq[Revision] = revisions
}
