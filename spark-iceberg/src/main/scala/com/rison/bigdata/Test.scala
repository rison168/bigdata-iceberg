package com.rison.bigdata

import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: Test
 * @USER: Rison
 * @DATE: 2022/9/27 23:10
 * @PROJECT_NAME: bigdata-iceberg
 * */

object Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName.stripSuffix("$"))

      .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")

      .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://hdfsCluster/apps/hive/warehouse")
      .getOrCreate()

    println(spark.conf.get("spark.sql.catalog.hadoop_catalog.warehouse"))
    spark.close()
  }
}
