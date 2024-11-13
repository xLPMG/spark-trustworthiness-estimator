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

object DataReader {
  def main(args: Array[String]): Unit = {
    // Check that a path argument is provided
    if (args.length < 1) {
      println("Usage: DataReader <path-to-folder-with-xml.bz2-files>")
      System.exit(1)
    }

    val folderPath = args(0)

    // Initialize SparkSession
    val spark = SparkSession
      .builder()
      .appName("DataReader")
      .getOrCreate()

    // Function to decompress and parse XML with SAX in a streaming manner
    def parseLargeXMLFile(filePath: String): Seq[Revision] = {
      // Create a FileSystem instance locally on each executor
      val hadoopConf = new Configuration()
      val fs = FileSystem.get(hadoopConf)

      // Open the BZip2 compressed file from Hadoop FileSystem
      val inputStream = fs.open(new Path(filePath))
      val bz2Stream = new BZip2CompressorInputStream(
        new BufferedInputStream(inputStream)
      )

      // Initialize SAX parser
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

      // Return the list of revisions extracted by the handler
      handler.revisions.toSeq
    }

    // Custom SAX Handler for parsing revisions
    class RevisionSAXHandler extends DefaultHandler {
      val revisions = ArrayBuffer[Revision]()
      var currentElement: String = ""
      var insidePage = false
      var insideRevision = false

      var pageId: String = ""
      var revisionId: String = ""
      var parentId: Option[String] = None
      var timestamp: String = ""

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
          case "revision" =>
            insideRevision = true
            parentId = None // Reset parentId for each new revision
          case _ => // No-op for other tags
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
          case "revision" =>
            // Add the extracted revision data to the list of revisions
            revisions += Revision(revisionId, pageId, parentId, timestamp)
            insideRevision = false
          case _ => // No-op for other tags
        }
        currentElement = ""
      }

      override def characters(
          ch: Array[Char],
          start: Int,
          length: Int
      ): Unit = {
        val content = new String(ch, start, length).trim

        if (insidePage && !insideRevision && currentElement == "id") {
          pageId = content // Page ID only needs to be set once per page
        }

        if (insideRevision) {
          currentElement match {
            case "id" if revisionId.isEmpty =>
              revisionId = content
            case "parentid" =>
              parentId = Some(content)
            case "timestamp" =>
              timestamp = content
            case _ => // No-op for other elements
          }
        }
      }
    }

    // Read all .xml.bz2 files in the folder into an RDD
    val filesRDD = spark.sparkContext.binaryFiles(s"$folderPath/*.bz2")

    // Process each file in the RDD to extract revisions
    val allRevisionsRDD = filesRDD.flatMap { case (path, _) =>
      parseLargeXMLFile(path)
    }

    // Action to trigger the processing, e.g., count the revisions extracted
    println(s"Total Revisions Extracted: ${allRevisionsRDD.count()}")

    // Stop Spark session
    spark.stop()
  }
}
