package nds.etl.parsers

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.GZIPInputStream

//import nds.etl.PgpUtils
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.collection.TraversableOnce


object HJParser {

  case class HJEvent(
    site_id: String,
    user_id: String,
    url: String
  )


  def parseFile( file: PortableDataStream): TraversableOnce[HJEvent] = {
    val stream = file.open
    val events = new BufferedReader(
      new InputStreamReader(
        new GZIPInputStream(stream))
    )
    .lines()
    .iterator()
    .asScala
    .map(line => {
      val fields = line.split(",")

      HJEvent(
        fields(0),
        fields(1),
        fields(2)

      )
    }).toArray

    stream.close()
    events
  }

  def normalizeEvents(hjEvents: DataFrame): DataFrame = {
    hjEvents
      .select(
        col("site_id"),
        col("user_id"),
        col("url") )
  }

  private


  // Use coordinate binning to construct CellIds, in case not available in data source
  // Two decimal accuracy provides roughly 1100m x 1100m squares
  val toCellId = udf((lat: Double, lng: Double) => {
    val latBin = (lat * 100).toInt
    val lngBin = (lng * 100).toInt

    if (latBin > Short.MaxValue || latBin < Short.MinValue ||
      lngBin > Short.MaxValue || lngBin < Short.MinValue ) {
      throw new IllegalArgumentException(s"Invalid coordinate values for 32bit CellId: lat ${latBin}, lng ${lngBin} ")
    }

    // Construct CellId as 32bit Int so that first 16 bits encode latitude and last 16 bits encode longitude
    latBin << 16 | (0x0000FFFF & lngBin)
  })


  val toTechnology = udf({vendor_technology: String => vendor_technology match {
    case "LUCENT_LTE" => 3
    case "LUCENT_3G1X" => 4
    case "SAMSUNG_3G1X" => 4
    case "LUCENT_EVDO" => 5
    case _ => -1
  }})



  val toTimezone = udf((local_time_zone: String) => local_time_zone match {
    case "EDT" => "America/New_York"
    case "EST" => "America/New_York"
    case tz => tz
  })

  val toEpochTime = udf((utc_event_time_epoch: Long) => { (utc_event_time_epoch / 1000).toInt })
}
