package com.rison.bigdata

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkDataFramWriteApplication
 * @USER: Rison
 * @DATE: 2022/9/29 16:23
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkDataFrameApiApplication {
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

    //TODO 准备数据
    val dataList = List[String](
      "{\"id\":1,\"name\":\"zs\",\"age\":18,\"loc\":\"beijing\"}",
      "{\"id\":2,\"name\":\"ls\",\"age\":19,\"loc\":\"shanghai\"}",
      "{\"id\":3,\"name\":\"ww\",\"age\":20,\"loc\":\"beijing\"}",
      "{\"id\":4,\"name\":\"ml\",\"age\":21,\"loc\":\"shanghai\"}"
    )
    import spark.implicits._
    val dataFrame: DataFrame = spark.read.json(dataList.toDS())

    //TODO 创建普通表df_test_tbl
    dataFrame.writeTo("hive_catalog.default.df_test_tbl").create()
    spark.read.table("hive_catalog.default.df_test_tbl").show()

    //TODO 创建分区表df_test_pt_tbl
    dataFrame
      .sortWithinPartitions($"loc")
      .writeTo("hive_catalog.default.df_test_pt_tbl")
      .partitionedBy($"loc")//这里可以联合分区
      .create()
    spark.read.table("hive_catalog.default.df_test_pt_tbl").show()
    spark.close()
  }
}

/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkDataFrameApiApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */
