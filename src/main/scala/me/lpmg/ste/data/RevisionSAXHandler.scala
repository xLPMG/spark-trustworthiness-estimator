package me.lpmg.ste.data

import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.{ArrayBuffer, StringBuilder}
import org.xml.sax.{Attributes, InputSource}

/** SAX handler for parsing MediaWiki XML revisions, filtering for specific
  * template and namespace.
  *
  * @param template
  *   The template to search for within page revisions
  */
class RevisionSAXHandler(template: String) extends DefaultHandler {
  private val revisions = ArrayBuffer[RevisionPair]()

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
          }
          currentTemplatePresent = true

          // no template anymore but we already set the first revision
        } else if (firstTemplateRevision.isDefined && !currentTemplatePresent) {
          // Second revision without template
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

          // Add the pair to revisions if both are present and the last revision has sources
          // Before checking for sources in the second revision, I noticed that it often happens
          // that all sources were removed with the template. Shortly after, the template and
          // all sources were added again. I guess this is some kind of vandalism that should
          // be ignored.
          for {
            first <- firstTemplateRevision
            last <- lastTemplateRevision
            if last.sources.nonEmpty
          } {

            // Create a new pair
            val pair = RevisionPair(
              revisionIdTemplateAdded = first.revisionId,
              revisionIdTemplateRemoved = last.revisionId,
              pageId = first.pageId,
              sourcesTemplateAdded = first.sources,
              sourcesTemplateRemoved = last.sources
            )

            revisions.append(pair)
          }

          // Reset for next potential pair
          firstTemplateRevision = None
          lastTemplateRevision = None
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
  def getRevisionPairs: Seq[RevisionPair] = revisions
}
