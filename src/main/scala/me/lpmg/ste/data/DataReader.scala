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

/**
  * Provides functionality to read and parse XML data from BZip2 compressed files.
  */
object DataReader {

    /**
      * Parse a BZip2 compressed XML file and extract revisions using SAX parser.
      *
      * @param filePath
      * @return
      */
    def parseXMLFile(filePath: String): Seq[Revision] = {
      // Create a FileSystem instance locally on each executor
      val hadoopConf = new Configuration()
      val fs = FileSystem.get(hadoopConf)

      // Open the BZip2 compressed file from Hadoop FileSystem
      val inputStream = fs.open(new Path(filePath))
      val bz2Stream = new BZip2CompressorInputStream(
        new BufferedInputStream(inputStream)
      )

      // Initialize parser
      val saxParserFactory = SAXParserFactory.newInstance()
      saxParserFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
      val xmlReader = saxParserFactory.newSAXParser().getXMLReader
      xmlReader.setProperty("http://www.oracle.com/xml/jaxp/properties/entityExpansionLimit", "1000000000")
      xmlReader.setProperty("http://www.oracle.com/xml/jaxp/properties/totalEntitySizeLimit", "1000000000")

      val handler = new RevisionSAXHandler()
      xmlReader.setContentHandler(handler)

      // Stream and parse the XML content
      try {
        xmlReader.parse(new InputSource(new InputStreamReader(bz2Stream)))
      } finally {
        // Close resources
        bz2Stream.close()
        inputStream.close()
        fs.close()
      }

      handler.revisions.toSeq
    }

}
