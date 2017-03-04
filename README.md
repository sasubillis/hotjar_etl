# Spark jobs for NDS ingestion pipeline

## Setup

1. Install [SBT](http://www.scala-sbt.org/index.html)
2. Download [Spark](http://spark.apache.org/downloads.html) for Scala 2.10


## Execution

Run at project root folder

Create new config file or config/Auckland.conf according to your needs.

Build and run the project.

    sbt assembly


    spark-submit \
      --master local[2] \
      --packages datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 \
      --conf spark.cassandra.connection.host=127.0.0.1 \
      --class nds.etl.jobs.StreamingEtl \
      target/scala-2.11/hotjar-ingestion-assembly-0.0.1-SNAPSHOT.jar config/hotjar-eu.conf
      
      
    spark-submit \
      --master local[2] \
      --packages datastax:spark-cassandra-connector:2.0.0-M2-s_2.11,org.apache.hadoop:hadoop-aws:2.7.3 \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.access.key=xxxx \
      --conf spark.hadoop.fs.s3a.secret.key=xxxxxxx \
      --class nds.etl.jobs.sprint.SprintBatchEtl \
      target/scala-2.11/hotjar-ingestion-assembly-0.0.1-SNAPSHOT.jar config/hotjar-eu.conf
