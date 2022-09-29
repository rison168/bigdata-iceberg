package com.rison.bigdata

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkStructuredStreamingApplication
 * @USER: Rison
 * @DATE: 2022/9/29 23:10
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkStructuredStreamingApplication {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      //指定hive catalog,catalog 命名为 hive_catalog
      .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hive_catalog.type", "hive")
      .config("spark.sql.catalog.hive_catalog.uri", "thrift://tbds-192-168-0-18:9083,thrift://tbds-192-168-0-29:9083")
      .config("iceberg.engine.hive.enabled", "true")
      //指定 hadoop catalog，catalog 命名为 hadoop_catalog
      .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
      .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://hdfsCluster/apps/hive/warehouse")
      //修改分区属性需要
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()
    //TODO 创建 iceberg 表
    spark.sql(
      """
        |create table if not exists hive_catalog.default.structure_stream_tbl(
        |id int,
        |name string,
        |age int,
        |loc string,
        |ts timestamp
        |) using iceberg
        |""".stripMargin)

    val checkpointPath = "hdfs://hdfsCluster/iceberg/spark/iceberg_table_checkpoint"
    val kafkaServers = "tbds-192-168-0-29:6669,tbds-192-168-0-30:6669,tbds-192-168-0-31:6669"
    val topic = "kafka_iceberg_topic"

    //TODO 读取kafka数据
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("kafka.security.protocol","SASL_PLAINTEXT")
      .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";")
      .option("kafka.sasl.mechanism","PLAIN")
      .option("auto.offset.reset", "earliest")
      .option("subscribe", topic)
      .load()
    import spark.implicits._
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.functions._
    var filterDF: DataFrame = kafkaDF
      .selectExpr("CAST(key AS string)", "CAST(value AS string)")
      .as[(String, String)]
      .filter(
        data => {
          data._2 != null && data._2 != ""
        }
      )
      .map(data => (data._1, data._2 + " " + System.currentTimeMillis()))
      .toDF("id", "data")

    val resFrame: DataFrame = filterDF
      .withColumn("id", split(col("data"), "\t")(0).cast("Int"))
      .withColumn("name", split(col("data"), "\t")(1).cast("String"))
      .withColumn("age", split(col("data"), "\t")(2).cast("Int"))
      .withColumn("loc", split(col("data"), "\t")(3).cast("String"))
      .withColumn("ts", from_unixtime(split(col("data"), "\t")(4), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
      .select("id", "name", "age", "loc","ts")

//    resFrame.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()

    //TODO 写iceberg表
    val query: StreamingQuery = resFrame
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
      .option("path", "hive_catalog.default.structure_stream_tbl")
      .option("fanout-enabled", "true")
      .option("checkpointLocation", checkpointPath)
      .start()
//    spark.sql("select * from hive_catalog.default.structure_stream_tbl")
    query.awaitTermination()


  }
}

/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkStructuredStreamingApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */