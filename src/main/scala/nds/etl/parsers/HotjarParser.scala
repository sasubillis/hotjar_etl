package nds.etl.parsers

/**
  * Created by ssasubil on 2/25/2017.
  */

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.zip.GZIPInputStream
import collection.JavaConverters._
import scala.util.control.Exception._
import org.apache.commons.csv.{CSVFormat, CSVParser}

case class HotjarEvent(
                        SITE_ID: String,
                        USER_ID: String,
                        URL: String,
                        TIMESTAMP: String,
                        WINDOW_SIZE: Int,
                        USER_AGENT: String,
                        LANGUAGE: String,
                        IPADDRESS: String,
                        TOOL_BEHAV_DATA: String,
                        CLICKS: Int
                      )


object HotjarParser extends Parser[HotjarEvent]
{


    def toIntOpt(str: String): Option[Int] = str match {
      case "" => None
      case n => catching(classOf[NumberFormatException]).opt(n.toInt)
    }


    def parse(inputStream: InputStream): Iterator[HotjarEvent] = {
      val csvFormat = CSVFormat.RFC4180.withHeader().withSkipHeaderRecord()
      val reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream)))
      val records = new CSVParser(reader, csvFormat).iterator().asScala

      records.map(record => {
        new HotjarEvent(
          record.get("SITE_ID"),
          record.get("USER_ID"),
          record.get("URL"),
          record.get("TIMESTAMP"),
          record.get("WINDOW_SIZE").toInt,
          record.get("USER_AGENT"),
          record.get("LANGUAGE"),
          record.get("IPADDRESS"),
          record.get("TOOL_BEHAV_DATA"),
          record.get("CLICKS").toInt

        )
      })
    }

    def parseFile(line: String): HotjarEvent = {
      val eventArray = line.split(",").map(_.trim)
      HotjarEvent(
        eventArray(0),
        eventArray(1),
        eventArray(2),
        eventArray(3),
        eventArray(4).toInt,
        eventArray(5),
        eventArray(6),
        eventArray(7),
        eventArray(8),
        eventArray(9).toInt


      )
    }
}