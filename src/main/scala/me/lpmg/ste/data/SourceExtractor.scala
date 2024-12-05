package me.lpmg.ste.data

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import java.net.URL
import scala.util.Try

/** Extracts sources from Wikipedia text content.
  * For URLs, only extracts the main domain or first subdomain.
  * For books, only extracts the ISBN.
  */
object SourceExtractor {
  // Regex patterns for source extraction
  private val RefTagPattern = """<ref[^>]*>(.*?)</ref>""".r
  private val CiteTemplatePattern = """\{\{(?i)(cite|citation|vcite2|vcite|vancite|wikicite|wayback)[^}]*\}\}""".r
  private val UrlPattern = """(?i)url\s*=\s*([^|\}]+)""".r
  private val IsbnPattern = """(?i)isbn\s*=\s*([^|\}]+)""".r
  private val UrlInRefPattern = """https?://[^\s<>"]+""".r
  private val ExternalLinkPattern = """\*\s*\[(https?://[^\s\]]+)[^\]]*\]""".r
  
  /** Extract domain from URL string.
    * Returns the two rightmost subdomains plus main domain if subdomains exist, otherwise returns the main domain.
    * If the URL starts with "www.", that part is removed.
    * Example: 
    * - three.two.one.example.com -> two.one.example.com
    */
  private def extractDomain(urlStr: String): Option[String] = {
    Try {
      val url = new URL(urlStr.trim)
      val host = url.getHost
      val parts = host.split("\\.")
      val domainParts = if (parts(0) == "www" || parts(0) == "www3") parts.tail else parts
      if (domainParts.length >= 3) {
        // For domains with subdomain(s), return rightmost subdomain plus main domain
        domainParts.takeRight(3).mkString(".").toLowerCase()
      } else if (domainParts.length == 2) {
        domainParts.takeRight(2).mkString(".").toLowerCase()
      } else {
        domainParts.mkString(".").toLowerCase()
      }
    }.toOption
  }

  /** Clean ISBN string by removing non-digit and non-X characters */
  private def cleanIsbn(isbn: String): String = {
    isbn.replaceAll("[^0-9X]", "")
  }
  
  /** Extract all sources from a Wikipedia text.
    * Returns a sequence of either domain names (for URLs) or ISBNs (for books).
    */
  def extractSources(text: String): Seq[String] = {
    val sources = ArrayBuffer[String]()
    
    // Extract from ref tags
    RefTagPattern.findAllMatchIn(text).foreach { m =>
      val refContent = m.group(1)
      // Extract URLs from ref content
      UrlInRefPattern.findAllMatchIn(refContent).foreach { urlMatch =>
        extractDomain(urlMatch.group(0)).foreach(sources += _)
      }
    }
    
    // Extract from cite templates
    CiteTemplatePattern.findAllMatchIn(text).foreach { m =>
      val template = m.group(0)
      
      // Extract URLs
      UrlPattern.findFirstMatchIn(template).foreach { urlMatch =>
        extractDomain(urlMatch.group(1)).foreach(sources += _)
      }
      
      // Extract ISBNs
      IsbnPattern.findFirstMatchIn(template).foreach { isbnMatch =>
        val isbn = cleanIsbn(isbnMatch.group(1))
        if (isbn.nonEmpty) {
          sources += s"ISBN:$isbn"
        }
      }
    }

    // Extract from external links section
    ExternalLinkPattern.findAllMatchIn(text).foreach { m =>
      extractDomain(m.group(1)).foreach(sources += _)
    }
    
    sources.distinct.toSeq
  }
}
