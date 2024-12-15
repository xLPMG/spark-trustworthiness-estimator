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
      dateLimit: Long = 0
  ): Seq[Revision] = {
    val saxParserFactory = SAXParserFactory.newInstance()
    saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)

    val xmlReader = setXMLReaderProperties(
      saxParserFactory.newSAXParser().getXMLReader
    )
    val handler = new RevisionSAXHandler(dateLimit)
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
      dateLimit: Long = 0
  ): Seq[Revision] = {
    Using.resource(pds.open()) { inputStream =>
      val bz2Stream =
        new BZip2CompressorInputStream(new BufferedInputStream(inputStream))
      getRevisions(bz2Stream, dateLimit)
    }
  }

  /**
    * Set properties for the XML reader
    *
    * @param xmlReader
    * @return
    */
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
}
