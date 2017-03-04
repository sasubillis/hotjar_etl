package nds.etl.sinks

import java.io.File
import java.net.InetAddress
import java.util
import java.util.UUID

import com.typesafe.config.Config
import nds.etl.Transformations
import org.apache.commons.io.FileUtils
import org.apache.cassandra.utils.NativeSSTableLoaderClient
import org.apache.cassandra.io.sstable.SSTableLoader
import org.apache.cassandra.utils.OutputHandler
import org.apache.cassandra.io.sstable.CQLSSTableWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Note: Create new Cassandra tables with NDS_infrastructure_setup/roles/cassandra-etl-schema whenever
  * new data sources are added. E.g.
  * - events_vodafone_nz
  * - users_vodafone_nz
  * - timeranges_vodafone_nz
  */
object Cassandra {


  def saveDataFrame(config: Config, enrichedEvents: DataFrame): Unit = {

    val tableSuffix = config.getString("nds.cassandra.tableSuffix")
    val cassandraConfig = Map(
      "cluster" -> config.getString("nds.cassandra.cluster"),
      "keyspace" -> config.getString("nds.cassandra.keyspace")
    )


    if (enrichedEvents.rdd.partitions.length > 0) {
      val events = toCassandraSchema(enrichedEvents)
      val users  = Transformations.toUsers(enrichedEvents)
      val minMaxEventTimes = Transformations.minMaxEventTimes(enrichedEvents)

      events.foreachPartition(savePartition(config, _))

      users
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode(SaveMode.Append)
        .options(cassandraConfig + ("table" -> s"users_$tableSuffix"))
        .save()

      minMaxEventTimes
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode(SaveMode.Append)
        .options(cassandraConfig + ("table" -> s"timeranges_$tableSuffix"))
        .save()
    }
  }


  private


  class SparkSSTableLoaderClient(hosts: util.Collection[InetAddress], port: Int, username: String, password: String)
    extends NativeSSTableLoaderClient(
      hosts,
      port,
      username,
      password,
      null) {
  }


  def toCassandraSchema(enrichedEvents: DataFrame): DataFrame = {
    enrichedEvents
      .select(
        col("user_id_seq"),
        col("event_time"),
        lit(0).as("point_location_id"),
        col("latitude"),
        col("longitude"),
        col("accuracy"),
        col("vendor_technology"),
        enrichedEvents.col("cell_id").as("cellid"),
        col("establishcause"),
        col("country_code"),
        col("technology"))
  }


  def savePartition(config: Config, partition: Iterator[Row]): Unit = {
    val root = System.getProperty("java.io.tmpdir")
    val namePrefix: String = "cassandra-bulk-loader"
    val tmpdir = new File(root, s"$namePrefix-${UUID.randomUUID.toString}/${config.getString("nds.cassandra.keyspace")}/events_${config.getString("nds.cassandra.tableSuffix")}")
    tmpdir.mkdirs()


    var count = 0
    var writer = getWriter(config, tmpdir)

    partition.foreach(row => {
      writer.addRow(seqAsJavaList(row.toSeq).asInstanceOf[java.util.List[AnyRef]])
      count += 1

      // CQLSSTableWriter gets exponentially slower when lot's of rows are added to it, so it needs to be recycled
      if (count == 100000) {
        count = 0
        writer.close()
        writer = getWriter(config, tmpdir)
      }
    })

    writer.close()
    loadToCassandra(config, tmpdir)
  }

  def getWriter(config: Config, tmpdir: File): CQLSSTableWriter = {
    val tableSuffix = config.getString("nds.cassandra.tableSuffix")

    val schema =
      s"""CREATE TABLE ndskeyspace.events_${tableSuffix}(
          |    user_id_seq         text,
          |    event_time          int,
          |    point_location_id   int ,
          |    latitude            double,
          |    longitude           double,
          |    accuracy            double,
          |    vendor_technology   text,
          |    cellid              int,
          |    establishcause      text,
          |    country_code        text,
          |    technology          int,
          |    PRIMARY KEY (user_id_seq, event_time)
          |);
      """.stripMargin


    val insertStatement =
      s"""INSERT INTO ndskeyspace.events_${tableSuffix} (
          |   user_id_seq,
          |   event_time,
          |   point_location_id,
          |   latitude,
          |   longitude,
          |   accuracy,
          |   vendor_technology,
          |   cellid,
          |   establishcause,
          |   country_code,
          |   technology
          |) VALUES (
          |   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
          |);
      """.stripMargin

    CQLSSTableWriter
      .builder()
      .forTable(schema)
      .using(insertStatement)
      .withBufferSizeInMB(256)
      .inDirectory(tmpdir)
      .build()
  }


  def loadToCassandra(config: Config, tmpdir: File) {
    val loader = new SSTableLoader(
      tmpdir,
      new SparkSSTableLoaderClient(
        config.getString("nds.cassandra.host").split(",").map(InetAddress.getByName).toSeq.asJava,
        config.getInt("nds.cassandra.port"),
        config.getString("nds.cassandra.username"),
        config.getString("nds.cassandra.password")),
      new OutputHandler.SystemOutput(false, false)
    )

    loader.stream().get()
    FileUtils.deleteDirectory(tmpdir.getParentFile.getParentFile)
  }



  def saveCassandraWithNativeMode(config: Config, enrichedEvents: DataFrame){
    val tableSuffix = config.getString("nds.cassandra.tableSuffix")
    val cassandraConfig = Map(
      "cluster" -> config.getString("nds.cassandra.cluster"),
      "keyspace" -> config.getString("nds.cassandra.keyspace")
    )

    if (enrichedEvents.rdd.partitions.length > 0) {
      //enrichedEvents.cache()
      val events = Cassandra.toCassandraSchema(enrichedEvents)
      val users  = Transformations.toUsers(enrichedEvents)
      val minMaxEventTimes  = Transformations.minMaxEventTimes(enrichedEvents)

      events
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode(SaveMode.Append)
        .options(cassandraConfig + ("table" -> s"events_$tableSuffix"))
        .save()

      users
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode(SaveMode.Append)
        .options(cassandraConfig + ("table" -> s"users_$tableSuffix"))
        .save()

      minMaxEventTimes
        .write
        .format("org.apache.spark.sql.cassandra")
        .mode(SaveMode.Append)
        .options(cassandraConfig + ("table" -> s"timeranges_$tableSuffix"))
        .save()
    }
  }
}
