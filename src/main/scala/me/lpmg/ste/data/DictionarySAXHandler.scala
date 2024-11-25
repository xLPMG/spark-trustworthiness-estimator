package me.lpmg.ste.data

import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.HashMap
import org.xml.sax.Attributes
import spire.std.char

/** SAX handler creating a dictionary of titles and their corresponding page
  * IDs.
  */
class DictionarySAXHandler extends DefaultHandler {
  // Page title -> (Page ID, Redirect title)
  private val dictionary = new HashMap[String, (Int, String)]()

  private var currentPageTitle: String = ""
  private var currentPageId: Int = -1
  private var currentRedirectTitle: Option[String] = None

  private var insidePage = false
  private var insideRevision = false

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
        currentPageTitle = ""
        currentPageId = -1
        currentRedirectTitle = None
      case "revision" => insideRevision = true
      case "redirect" =>
        if (insidePage && !insideRevision) {
          val title = attributes.getValue("title")
          if (title != null && title.length() > 0) {
            currentRedirectTitle = Some(title)
          }
        }
      case _ => // do nothing
    }
  }

  override def endElement(
      uri: String,
      localName: String,
      qName: String
  ): Unit = {
    // page specific
    if (!insideRevision && insidePage) {
      qName match {
        case "page" =>
          insidePage = false
          if (currentPageTitle.length > 0 && currentPageId >= 0) {
            dictionary += (currentPageTitle -> (
              currentPageId,
              currentRedirectTitle.getOrElse("")
            ))
          }
        case "title" => currentPageTitle = getBuffer
        case "id"    => currentPageId = getBuffer.toInt
        case _       => // do nothing
      }
    } else {
      qName match {
        case "revision" =>
          insideRevision = false
        case _ => // do nothing
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

  private def getBuffer: String = charBuffer.toString.trim

  def getDictionary: Types.DictType = dictionary.toMap
}
