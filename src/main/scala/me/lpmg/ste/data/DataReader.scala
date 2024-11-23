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

  /** Parse an XML input stream to retrieve a sequence of revisions.
    *
    * @param inputStream
    *   XML input stream
    * @return
    *   Sequence of revisions
    */
  def getRevisions(
      inputStream: InputStream,
      dictionary: Map[String, (Long, String)]
  ): Seq[Revision] = {
    val saxParserFactory = SAXParserFactory.newInstance()
    saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)

    val xmlReader = setXMLReaderProperties(
      saxParserFactory.newSAXParser().getXMLReader
    )

    val handler = new RevisionSAXHandler()
    handler.setDictionary(dictionary)
    xmlReader.setContentHandler(handler)

    val inputSource = new InputSource(
      new InputStreamReader(inputStream, "UTF-8")
    )
    xmlReader.parse(inputSource)
    handler.getRevisions
  }

  /** Parse a (bz2 zipped) XML PortableDataStream to retrieve a sequence of
    * revisions. The dictionary map is used to directly resolve page titles to
    * page IDs.
    *
    * @param pds
    *   PortableDataStream
    * @param dictionary
    *   Dictionary map
    * @return
    *   Sequence of revisions
    */
  def getRevisionsFromPDS(
      pds: PortableDataStream,
      dictionary: Map[String, (Long, String)]
  ): Seq[Revision] = {
    Using.resource(pds.open()) { inputStream =>
      val bz2Stream =
        new BZip2CompressorInputStream(new BufferedInputStream(inputStream))
      getRevisions(bz2Stream, dictionary)
    }
  }

  /** Parse an XML input stream to create a dictionary map containing "Page
    * Title -> Page ID". The dictionary map is used to directly resolve page
    * titles to page IDs.
    *
    * @param inputStream
    *   XML input stream
    * @return
    *   Dictionary map
    */
  def getDictionary(inputStream: InputStream): Map[String, (Long, String)] = {
    val saxParserFactory = SAXParserFactory.newInstance()
    saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)

    val xmlReader = setXMLReaderProperties(
      saxParserFactory.newSAXParser().getXMLReader
    )

    val handler = new DictionarySAXHandler()
    xmlReader.setContentHandler(handler)

    val inputSource = new InputSource(
      new InputStreamReader(inputStream, "UTF-8")
    )
    xmlReader.parse(inputSource)
    handler.getDictionary
  }

  /** Parse a (bz2 zipped) XML PortableDataStream to create a dictionary map
    * containing "Page Title -> Page ID".
    *
    * @param pds
    *   PortableDataStream
    * @return
    *   Dictionary map
    */
  def getDictionaryFromPDS(
      pds: PortableDataStream
  ): Map[String, (Long, String)] = {
    Using.resource(pds.open()) { inputStream =>
      val bz2Stream =
        new BZip2CompressorInputStream(new BufferedInputStream(inputStream))
      getDictionary(bz2Stream)
    }
  }

  private final def setXMLReaderProperties(xmlReader: XMLReader): XMLReader = {
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
}
