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
  private val RefTagPattern =
    """(?i)(?:<ref[^>]*>(.*?)</ref>|&lt;ref[^&]*&gt;(.*?)&lt;/ref&gt;)""".r
  private val CiteTemplatePattern =
    """\{\{(?i)(cite|citation|vcite2|vcite|vancite|wikicite|wayback)[^}]*\}\}""".r
  private val UrlPattern = """(?i)url\s*=\s*([^|\}]+)""".r
  private val IsbnPattern = """(?i)isbn\s*=\s*([^|\}]+)""".r
  private val UrlInRefPattern = """https?://[^\s<>"]+""".r

  // Initialize PublicSuffixList
  private val publicSuffixList: PublicSuffixList =
    new PublicSuffixListFactory().build()

  def extractDomain(urlStr: String): Option[String] = {
    Try {
      val trimmedUrlStr = urlStr.trim
      val lastTwoChars = trimmedUrlStr.takeRight(2)
      val lastChar = trimmedUrlStr.last

      val cleanedUrlStr =
        if ("}}".equals(lastTwoChars) || "]]".equals(lastTwoChars))
          trimmedUrlStr.dropRight(2)
        else if (
          ",".equals(lastChar) || "]"
            .equals(lastChar) || ")".equals(lastChar) || "}".equals(lastChar)
        )
          trimmedUrlStr.dropRight(1)
        else trimmedUrlStr
      if (cleanedUrlStr.isEmpty() || cleanedUrlStr.contains("{{")) {
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
    *
    * Note: Wikiepdia says that cites should be enclosed in ref tags as well.
    * However in practice, I have seen many revisions where editors did not
    * follow this rule. So, I am also extracting cites using cite templates.
    * This means that in many cases, we will match the same source multiple
    * times, however by using .distinct at the end, we only keep unique sources.
    */
  def extractSources(text: String): Seq[String] = {
    val sources = ArrayBuffer[String]()

    if (text != null) {
      // Extract from ref tags
      RefTagPattern.findAllMatchIn(text).foreach { m =>
        val refContent = m.group(1)
        if (refContent != null) {
          // Extract URLs from ref content
          UrlInRefPattern.findAllMatchIn(refContent).foreach { urlMatch =>
            extractDomain(urlMatch.group(0)).foreach(sources += _)
          }
        }
      }

      // Extract from cite templates
      CiteTemplatePattern.findAllMatchIn(text).foreach { m =>
        val template = m.group(0)
        if (template != null) {
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
      }
    }

    sources.distinct
  }
}
