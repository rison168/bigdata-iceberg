package com.rison.bigdata

import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkDeleteApplication
 * @USER: Rison
 * @DATE: 2022/9/28 16:45
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkDeleteApplication {
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

    spark.sql("drop table if exists hive_catalog.default.delete_tbl")
    spark.sql(
      """
        |create table hive_catalog.default.delete_tbl(
        |id int,
        |name string,
        |age int
        |) using iceberg
        |
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into hive_catalog.default.delete_tbl
        |values
        |(1, 'rison', 18),
        |(2, 'zhagnsan', 19),
        |(3, 'lisi', 20),
        |(4, 'box', 22),
        |(5, 'tbds', 23),
        |(6, 'seabox', 25),
        |(7, 'kafka', 26),
        |(8, 'hive', 27),
        |(9, 'iceberg', 10)
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.delete_tbl").show()
    spark.sql("delete from hive_catalog.default.delete_tbl where age >= 25")
    spark.sql("select * from hive_catalog.default.delete_tbl").show()

    spark.close()
  }

}
/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkDeleteApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */