package nds.etl.jobs

import java.io.File

import com.typesafe.config.ConfigFactory
import nds.etl.TransformationsHJ
import nds.etl.parsers.{HotjarEvent, HotjarParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object HotjarStreamingEtl {

  def main(args: Array[String]) {
    val config = ConfigFactory.parseFile(new File(args(0)))

    val batchInterval = Seconds(config.getString("nds.ingestion.batchInterval").toInt)
    val sparkConf = new SparkConf()
    val cassandraHost = config.getString("nds.cassandra.host")
    val cassandraPort = config.getString("nds.cassandra.port")
    sparkConf.set("spark.cassandra.connection.host", cassandraHost)
    sparkConf.set("spark.cassandra.connection.port", cassandraPort)
    sparkConf.set("spark.cassandra.output.concurrent.writes" ,"40")
    sparkConf.set("spark.cassandra.output.throughput_mb_per_sec" ,"256")


    val ssc = new StreamingContext(sparkConf, batchInterval)
    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    import sqlContext.implicits._


//    val reasonCodes = broadcast(sqlContext
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load(config.getString("nds.ingestion.reasons")))


//    val cells = broadcast(sqlContext
//      .read
//      .json(config.getString("nds.ingestion.cells"))
//      .select(
//        $"cellId".cast(IntegerType).as("cell_id"),
//        $"rncId",
//        $"trueServingCoverageArea.longitude".cast(DoubleType).as("longitude"),
//        $"trueServingCoverageArea.latitude".cast(DoubleType).as("latitude"),
//        $"trueServingCoverageArea.semimajor".cast(DoubleType).as("accuracy"))
//      .filter(
//        $"latitude".isNotNull and $"longitude".isNotNull and $"cell_id".isNotNull))


    val rawDataStream = ssc.textFileStream(config.getString("hdfs-input-directory"))
    val stream: DStream[HotjarEvent] = rawDataStream.transform(rdd =>
      rdd.filter(line => line(0) != ' ' && line.split(",").length == 10).map(HotjarParser.parseFile(_)))

    stream.foreachRDD { rdd =>
      if (rdd.partitions.length > 0) {
        val hotjarEvents = rdd.toDF()
        val normalizedEvents = TransformationsHJ.normalizeEvents(config, hotjarEvents)
        //val enrichedEvents = TransformationsHJ.enrichEvents(config,  normalizedEvents)

        //Cassandra.saveDataFrame(config, enrichedEvents)
        //Reverted Save to Cassandra with Native API
        //Cassandra.saveCassandraWithNativeMode(config, normalizedEvents)
        normalizedEvents.write.mode(SaveMode.Append).parquet("hdfs://coord-1/nds-etl/output/hotjarevents.parquet")


      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

