package com.rison.bigdata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.iceberg.Table
import org.apache.iceberg.actions.Actions
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.ConfigProperties
import org.apache.iceberg.hive.HiveCatalog
import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkCompactDataFiles
 * @USER: Rison
 * @DATE: 2022/9/27 22:36
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkCompactDataFilesApplication {
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

    //TODO set hive catalog
    val hadoopConfiguration: Configuration = spark.sparkContext.hadoopConfiguration
    //iceberg.engine.hive.enabled=true
    hadoopConfiguration.set(ConfigProperties.ENGINE_HIVE_ENABLED, "true")
    hadoopConfiguration.set("client.pool.cache.eviction-interval-ms", "60000")
    hadoopConfiguration.set("clients", "5")
    hadoopConfiguration.set("uri", spark.conf.get("spark.sql.catalog.hive_catalog.uri"))
    hadoopConfiguration.set("warehouse", spark.conf.get("spark.sql.catalog.hadoop_catalog.warehouse"))
    hadoopConfiguration.set("fs.defaultFS", "hdfs://hdfsCluster")
    hadoopConfiguration.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/hdfs-site.xml"))
    hadoopConfiguration.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/core-site.xml"))
    hadoopConfiguration.addResource(new Path("/usr/hdp/current/hive-client/conf/hive-site.xml"))
    hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    //这里默认本地环境是simple认证
    hadoopConfiguration.set("hadoop.security.authentication", "simple")
    hadoopConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true)
    UserGroupInformation.setConfiguration(hadoopConfiguration)
    UserGroupInformation.loginUserFromSubject(null)
    hadoopConfiguration.set("property-version", "1")
    val hivecatalog = new HiveCatalog(hadoopConfiguration)
    val table: Table = hivecatalog.loadTable(TableIdentifier.of("default", "iceberg_test_tbl"))
    Actions.forTable(table).rewriteDataFiles().targetSizeInBytes(1024*1024*128).execute() //128m
    table.expireSnapshots().expireOlderThan(1664292360000L).commit()

    spark.close()

  }
}
/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkCompactDataFilesApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */