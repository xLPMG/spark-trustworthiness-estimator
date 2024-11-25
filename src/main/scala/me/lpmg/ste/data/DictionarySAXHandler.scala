package me.lpmg.ste.data

import org.xml.sax.helpers.DefaultHandler
import scala.collection.mutable.HashMap
import org.xml.sax.Attributes

/** SAX handler creating a dictionary of titles and their corresponding page
  * IDs.
  */
class DictionarySAXHandler extends DefaultHandler {
  // Page title -> (Page ID, Redirect title)
  private val dictionary = new HashMap[String, (Long, String)]()
  private var currentElement: String = ""
  private var currentPageTitle: String = ""
  private var currentPageId: Option[Long] = None
  private var currentRedirectTitle: Option[String] = None
  private var insidePage = false
  private var insideRevision = false

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
        currentPageTitle = ""
        currentPageId = None
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
    qName match {
      case "page" =>
        insidePage = false
        if (currentPageTitle.length > 0 && currentPageId.isDefined) {
          dictionary += (currentPageTitle -> (
            currentPageId.get,
            currentRedirectTitle.getOrElse("")
          ))
        }
      case "revision" =>
        insideRevision = false
      case _ => // do nothing
    }
    currentElement = ""
  }

  override def characters(ch: Array[Char], start: Int, length: Int): Unit = {
    val content = new String(ch, start, length).trim

    if (insidePage && !insideRevision) {
      currentElement match {
        case "title" => currentPageTitle = content
        case "id"    => currentPageId = Some(content.toLong)
        case _       => // do nothing
      }
    }
  }

  def getDictionary: Map[String, (Long, String)] = dictionary.toMap
}
