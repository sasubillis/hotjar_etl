package nds.etl

import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.DataFrame


object TransformationsHJ {




  def normalizeEvents(config: Config, rawEvents: DataFrame): DataFrame = config.getString("hotjar.ingestion.datasource") match {
    case "site1" => normalizeSite1HotjarEvents(config, rawEvents)
  }


  def normalizeSite1HotjarEvents(config: Config, hotjarEvents: DataFrame): DataFrame = {
    val tz = config.getString("hotjar.ingestion.input.tz")



    hotjarEvents
      .select(
        col("SITE_ID").as("site_id"),
        col("USER_ID").as("user_id"),
        col("URL").as("url"),
        unix_timestamp(to_utc_timestamp(col("TIMESTAMP"), tz)).cast(IntegerType).as("time_stamp"),
        col("WINDOW_SIZE").as("window_size"),
        col("USER_AGENT").as("user_agent"),
        col("LANGUAGE").as("language"),
        col("IPADDRESS").as("ipaddress"),
        col("TOOL_BEHAV_DATA").as("tool_behav_data"),
        col("CLICKS").as("clicks")
      )
  }



/*  def enrichEvents(config: Config,  normalizedEvents: DataFrame): DataFrame = config.getString("hotjar.ingestion.datasource") match {
    case "site1" => {
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
  }*/


}
