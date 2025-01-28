package me.lpmg.ste.data

import org.apache.spark.sql.SparkSession
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import java.io.{BufferedInputStream, InputStreamReader}
import javax.xml.parsers.{SAXParser, SAXParserFactory}
import org.xml.sax.helpers.DefaultHandler
import org.xml.sax.{Attributes, InputSource}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ArrayBuffer
import javax.xml.XMLConstants
import org.xml.sax.XMLReader
import java.io.InputStream
import scala.util.Using
import org.apache.spark.input.PortableDataStream
import com.typesafe.scalalogging.Logger

/** Provides functionality to read and parse XML data from BZip2 compressed
  * files.
  */
object DataReader {

  def getRevisionPairs(
      inputStream: InputStream,
      template: String,
      inline: Boolean = false
  ): Seq[RevisionPair] = {
    val saxParserFactory = SAXParserFactory.newInstance()
    saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)

    val xmlReader = setXMLReaderProperties(
      saxParserFactory.newSAXParser().getXMLReader
    )
    if (inline) {
      val handler = new RevisionSAXHandlerInline(template)
      xmlReader.setContentHandler(handler)

      val inputSource = new InputSource(
        new InputStreamReader(inputStream, "UTF-8")
      )
      xmlReader.parse(inputSource)
      handler.getRevisionPairs
    } else {
      val handler = new RevisionSAXHandler(template)
      xmlReader.setContentHandler(handler)

      val inputSource = new InputSource(
        new InputStreamReader(inputStream, "UTF-8")
      )
      xmlReader.parse(inputSource)
      handler.getRevisionPairs
    }
  }

  def getRevisionsFromPDS(
      pds: PortableDataStream,
      template: String,
      inline: Boolean = false
  ): Seq[Revision] = {
    Using.resource(pds.open()) { inputStream =>
      val bz2Stream =
        new BZip2CompressorInputStream(new BufferedInputStream(inputStream))
      getRevisions(bz2Stream, template, inline)
    }
  }

  def getRevisions(
      inputStream: InputStream,
      template: String,
      inline: Boolean = false
  ): Seq[Revision] = {
    revisionPairsToRevisions(getRevisionPairs(inputStream, template, inline))
  }

  def getRevisionPairsFromPDS(
      pds: PortableDataStream,
      template: String,
      inline: Boolean = false
  ): Seq[RevisionPair] = {
    Using.resource(pds.open()) { inputStream =>
      val bz2Stream =
        new BZip2CompressorInputStream(new BufferedInputStream(inputStream))
      getRevisionPairs(bz2Stream, template, inline)
    }
  }

  private def setXMLReaderProperties(xmlReader: XMLReader): XMLReader = {
    xmlReader.setProperty(
      "http://www.oracle.com/xml/jaxp/properties/entityExpansionLimit",
      "-1"
    )
    xmlReader.setProperty(
      "http://www.oracle.com/xml/jaxp/properties/totalEntitySizeLimit",
      "-1"
    )
    xmlReader
  }

  def revisionPairsToRevisions(
      revisionPairs: Seq[RevisionPair]
  ): Seq[Revision] = {
    revisionPairs.flatMap { pair =>
      val revision_1 = Revision(
        pair.revisionIdTemplateAdded,
        pair.revisionIdTemplateRemoved,
        pair.pageId,
        true,
        false,
        true,
        false,
        pair.sourcesTemplateAdded
      )
      val revision_2 = Revision(
        pair.revisionIdTemplateRemoved,
        pair.revisionIdTemplateAdded,
        pair.pageId,
        false,
        true,
        false,
        true,
        pair.sourcesTemplateRemoved
      )
      Seq(revision_1, revision_2)
    }
  }
}
