package me.lpmg.ste.data

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import java.net.URL
import scala.util.Try
import de.malkusch.whoisServerList.publicSuffixList.PublicSuffixList
import de.malkusch.whoisServerList.publicSuffixList.PublicSuffixListFactory

/** Extracts sources from Wikipedia text content. For URLs, only extracts the
  * main domain or first subdomain. For books, only extracts the ISBN.
  */
object SourceExtractor {
  // Regex patterns for source extraction
  private val RefTagPattern = """<ref[^>]*>(.*?)</ref>""".r
  private val CiteTemplatePattern =
    """\{\{(?i)(cite|citation|vcite2|vcite|vancite|wikicite|wayback)[^}]*\}\}""".r
  private val UrlPattern = """(?i)url\s*=\s*([^|\}]+)""".r
  private val IsbnPattern = """(?i)isbn\s*=\s*([^|\}]+)""".r
  private val UrlInRefPattern = """https?://[^\s<>"]+""".r
  private val ExternalLinkPattern = """\*\s*\[(https?://[^\s\]]+)[^\]]*\]""".r
 
  // Initialize PublicSuffixList
  private val publicSuffixList: PublicSuffixList =
    new PublicSuffixListFactory().build()

  private def extractDomain(urlStr: String): Option[String] = {
    Try {
      val trimmedUrlStr = urlStr.trim
      val cleanedUrlStr =
        if (trimmedUrlStr.endsWith("}}")) trimmedUrlStr.dropRight(2)
        else trimmedUrlStr
      if (cleanedUrlStr.isEmpty()) {
        return None
      }

      val url = new URL(cleanedUrlStr)
      val host = url.getHost
      val domain = publicSuffixList.getRegistrableDomain(host).toLowerCase()
      if (null == domain) {
        return None
      } else {
        return Some(domain)
      }
    }.toOption
  }

  /** Clean ISBN string by removing non-digit and non-X characters */
  private def cleanIsbn(isbn: String): String = {
    isbn.replaceAll("[^0-9X]", "")
  }

  /** Extract all sources from a Wikipedia text. Returns a sequence of either
    * domain names (for URLs) or ISBNs (for books).
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

    // Clean up links
    val cleanedSources = sources.map { source =>
      val pipeIndex = source.indexOf('|')
      if (pipeIndex >= 0) source.substring(0, pipeIndex) else source
    }

    cleanedSources.distinct
  }
}
