package me.lpmg.ste.data

import com.typesafe.scalalogging.Logger
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.xml.sax.Attributes
import org.xml.sax.InputSource
import org.xml.sax.XMLReader
import org.xml.sax.helpers.DefaultHandler

import java.io.BufferedInputStream
import java.io.InputStream
import java.io.InputStreamReader
import javax.xml.XMLConstants
import javax.xml.parsers.SAXParser
import javax.xml.parsers.SAXParserFactory
import scala.collection.mutable.ArrayBuffer
import scala.util.Using

/** Provides functionality to read and parse XML data from BZip2 compressed
  * files.
  */
object DataReader {

  /** Parses XML data from an InputStream and returns a sequence of RevisionPair
    * objects.
    *
    * @param inputStream
    *   The input stream containing the XML data.
    * @param template
    *   The template string to be used for parsing.
    * @param inline
    *   A flag indicating whether to use inline parsing.
    * @return
    *   A sequence of RevisionPair objects.
    */
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

  /** Reads and parses XML data from a PortableDataStream and returns a sequence
    * of Revision objects.
    *
    * @param pds
    *   The PortableDataStream containing the XML data.
    * @param template
    *   The template string to be used for parsing.
    * @param inline
    *   A flag indicating whether to use inline parsing.
    * @return
    *   A sequence of Revision objects.
    */
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

  /** Parses XML data from an InputStream and returns a sequence of Revision
    * objects.
    *
    * @param inputStream
    *   The input stream containing the XML data.
    * @param template
    *   The template string to be used for parsing.
    * @param inline
    *   A flag indicating whether to use inline parsing.
    * @return
    *   A sequence of Revision objects.
    */
  def getRevisions(
      inputStream: InputStream,
      template: String,
      inline: Boolean = false
  ): Seq[Revision] = {
    revisionPairsToRevisions(getRevisionPairs(inputStream, template, inline))
  }

  /** Reads and parses XML data from a PortableDataStream and returns a sequence
    * of RevisionPair objects.
    *
    * @param pds
    *   The PortableDataStream containing the XML data.
    * @param template
    *   The template string to be used for parsing.
    * @param inline
    *   A flag indicating whether to use inline parsing.
    * @return
    *   A sequence of RevisionPair objects.
    */
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

  /** Converts a sequence of RevisionPair objects to a sequence of Revision
    * objects.
    *
    * @param revisionPairs
    *   The sequence of RevisionPair objects.
    * @return
    *   A sequence of Revision objects.
    */
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

  /** Converts a sequence of RevisionPa‚ir objects to a sequence of Revision
    * objects in a distributed manner.
    *
    * @param revisionPairs
    *   The sequence of RevisionPair objects.
    * @return
    *   A sequence of Revision objects.
    */
  def revisionPairsToRevisionsDistributed(
      revisionPairs: RDD[RevisionPair]
  ): RDD[Revision] = {
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
