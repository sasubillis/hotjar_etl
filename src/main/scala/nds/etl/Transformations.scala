package nds.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import org.apache.spark.sql.types.IntegerType


object Transformations {

  case class Retina3gEvent(
    event_time: Int,
    call_id: Long,
    user_id_seq: String,
    cell_id: Int,
    latitude: Double,
    longitude: Double,
    accuracy: Double,
    establishcause: String,
    country_code: String,
    technology: Int
  )


  def normalizeEvents(config: Config, rawEvents: DataFrame): DataFrame = config.getString("nds.ingestion.datasource") match {
    case "retina3g" => normalizeRetina3gEvents(config, rawEvents)
    case "traffica-rnc" => normalizeTrafficaRncEvents(config, rawEvents)
  }


  def normalizeTrafficaRncEvents(config: Config, trafficaRncEvents: DataFrame): DataFrame = {
    val tz = config.getString("nds.ingestion.input.tz")
    val country = config.getString("nds.ingestion.country")
    val technology = config.getInt("nds.ingestion.technology")

    trafficaRncEvents
      .select(
        unix_timestamp(to_utc_timestamp(col("REPORT_TIME"), tz)).cast(IntegerType).as("event_time"),
        Udf.callId(col("RRC_IMSI"), col("RRC_SRNTI")).as("call_id"),
        col("RRC_IMSI").as("hashed_imsi"),
        col("RRC_IMSI").as("user_id_seq"), // Use sha256 hash as a user id. Hashing done by the customer team
        lit("Nokia_TrafficaRNC").as("vendor_technology"),
        Udf.uCellId(col("RNC_ID"), coalesce(col("RRC_INFO_STOP_CID"), col("RAB_DATA_STOP_CID"))).as("cell_id"),
        coalesce(col("RRC_INFO_IN_CAUSE_REASON"), col("RAB_DATA_IN_CAUSE_REASON")).as("reason_code"),
        lit(country).as("country_code"),
        lit(technology).as("technology"))
  }


  def normalizeRetina3gEvents(config: Config, retina3gEvents: DataFrame): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val country = config.getString("nds.ingestion.country")
    val technology = config.getInt("nds.ingestion.technology")


    retina3gEvents
      .flatMap(row => {
        List(
          Retina3gEvent(
            (row.getAs[Long]("StartTime") / 1000).toInt,
            row.getAs[Long]("CallId"),
            row.getAs[String]("HashedIMSI"),
            row.getAs[Int]("StartCellId"),
            row.getAs[Double]("Latitude"),
            row.getAs[Double]("Latitude"),
            row.getAs[Double]("Accuracy"),
            row.getAs[String]("EstablishCause"),
            country,
            technology
          ),


          Retina3gEvent(
            (row.getAs[Long]("EndTime") / 1000).toInt,
            row.getAs[Long]("CallId"),
            row.getAs[String]("HashedIMSI"),
            row.getAs[Int]("EndCellId"),
            row.getAs[Double]("Latitude"),
            row.getAs[Double]("Latitude"),
            row.getAs[Double]("Accuracy"),
            row.getAs[String]("EstablishCause"),
            country,
            technology
          )
        )
      })
      .toDF()
  }


  def enrichEvents(config: Config, cells: DataFrame, reasonCodes: DataFrame, normalizedEvents: DataFrame): DataFrame = config.getString("nds.ingestion.datasource") match {
    case "retina3g" => normalizedEvents
    case "sprint"   => normalizedEvents
    case "traffica-rnc" => {
      normalizedEvents
        .join(reasonCodes, normalizedEvents.col("reason_code") === reasonCodes.col("REASON_CODE"), "left_outer")
        .join(cells, "cell_id")
        .withColumnRenamed("REASON_NAME", "establishcause")
        .filter(
          col("event_time").isNotNull and
            col("user_id_seq").isNotNull and
            col("latitude").isNotNull and
            col("longitude").isNotNull)
    }
  }


  def minMaxEventTimes(enrichedEvents: DataFrame): DataFrame = {
    enrichedEvents
      .select(col("event_time"))
      .agg(
        min(col("event_time")).as("min_event_time"),
        max(col("event_time")).as("max_event_time"))
  }


  def toUsers(enrichedEvents: DataFrame): DataFrame = {
    enrichedEvents
      .select("user_id_seq")
      .distinct()
  }
}
