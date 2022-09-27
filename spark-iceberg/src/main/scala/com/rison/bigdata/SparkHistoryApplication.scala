package com.rison.bigdata

import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkHistoryApplication
 * @USER: Rison
 * @DATE: 2022/9/27 14:17
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkHistoryApplication {
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
    spark.sql(
      """
        |create table if not exists hive_catalog.default.iceberg_test_tbl(id int, name string, age int) using iceberg
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.iceberg_test_tbl
        |values
        |(1,'rison', 18),
        |(2, 'zhangsan', 20)
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()
    spark.sql(
      """
        |select * from hive_catalog.default.iceberg_test_tbl.history
        |""".stripMargin
    ).show()

    spark.close()
  }
}
/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkHistoryApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */