package nds.etl.jobs

import java.io._

import com.typesafe.config.ConfigFactory
import nds.etl.parsers.HJParser
import org.apache.spark.sql.SparkSession

/**
  * @deprecated Use streaming ETL, if data is produced file-by-file to HDFS/S3.
  *             Batch ETL is useful only for testing, reruns and if the data provider as a batch.
  */
object HJBatchEtl {

  def main(args: Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .appName("SprintBatchEtl")
      .getOrCreate()

    import spark.implicits._

    val config = ConfigFactory.parseFile(new File(args(0)))
    val pgpPrivateKey = sys.env("PGP_PRIVATE_KEY")
    val pgpPrivateKeyPassword =  sys.env("PGP_PRIVATE_KEY_PASSWORD")


    val files = spark
      .sparkContext
      .binaryFiles(config.getString("nds.hdfs.input.directory"))

    // Shuffle the file names to the cluster before downloading the content from S3
    val distributedFiles = files.repartition(files.count.toInt)


    val hjEvents = distributedFiles
      .flatMap({ case (filename, file) => HJParser.parseFile( file) })
      .toDF

    val normalizedEvents = HJParser.normalizeEvents(hjEvents)

    //Cassandra.saveDataFrame(config, normalizedEvents)
    normalizedEvents.write.parquet("data.parquet")
  }
}
