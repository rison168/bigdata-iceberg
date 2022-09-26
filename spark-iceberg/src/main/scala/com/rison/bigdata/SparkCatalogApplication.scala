package com.rison.bigdata

import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkCatalogApplication
 * @USER: Rison
 * @DATE: 2022/9/26 17:52
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkCatalogApplication {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName.stripSuffix("$"))
      //指定hive catalog,catalog 命名为 hive_catalog
      .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hive_catalog.type", "hive")
      .config("spark.sql.catalog.hive_prod.uri", "thrift://tbds-192-168-0-18:9083,thrift://tbds-192-168-0-29:9083")
      .config("iceberg.engine.hive.enabled", "true")
      //指定 hadoop catalog，catalog 命名为 hadoop_catalog
      .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
      .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://hdfsCluster/apps/hive/warehouse")
      .getOrCreate()


        //TODO 1.1 使用hive catalog
        spark.sql(
          """
            |create table if not exists hive_catalog.default.test_tbl(
            |id int,
            |name string,
            |age int)
            |using iceberg
            |
            |""".stripMargin
        )
        //TODO 1.2 插入数据
        spark.sql(
          """
            |insert into hive_catalog.default.test_tbl values (1,'rison',20),(2,'zhangsan',21),(3,'lisi',22)
            |""".stripMargin
        )
        //TODO 1.3 查询数据数据
        spark.sql(
          """
            |select * from hive_catalog.default.test_tbl
            |""".stripMargin
        ).show()


    //TODO 2.1 使用hadoop catalog
    spark.sql(
      """
        |create table if not exists hadoop_catalog.default.test_tbl(
        |id int,
        |name string,
        |age int)
        |using iceberg
        |
        |""".stripMargin
    )
    //TODO 2.2 插入数据
    spark.sql(
      """
        |insert into hadoop_catalog.default.test_tbl values (1,'rison',20),(2,'zhangsan',21),(3,'lisi',22)
        |""".stripMargin
    )
    //TODO 2.3 查询数据数据
    spark.sql(
      """
        |select * from hadoop_catalog.default.test_tbl
        |""".stripMargin
    ).show()

    spark.close()
  }
}

/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkCatalogApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */