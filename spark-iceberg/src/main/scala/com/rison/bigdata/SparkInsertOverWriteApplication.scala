package com.rison.bigdata

import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkInsertOverWriteApplication
 * @USER: Rison
 * @DATE: 2022/9/28 14:37
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkInsertOverWriteApplication {
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
      .getOrCreate()

    spark.sql("drop table if exists hive_catalog.default.over_write_tbl")
    spark.sql("drop table if exists hive_catalog.default.over_write_tbl2")
    spark.sql("drop table if exists hive_catalog.default.over_write_tbl3")
    //t1 分区表
    spark.sql(
      """
        |create table if not exists hive_catalog.default.over_write_tbl(
        |id int,
        |name string,
        |loc string
        |)using iceberg
        |partitioned by (loc)
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into hive_catalog.default.over_write_tbl
        |values
        |(1,'rison','beijing'),
        |(2,'zhangsan', 'guangzhou'),
        |(3, 'lisi', 'shagnhai')
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.over_write_tbl").show()
    //t2 不分区表
    spark.sql(
      """
        |create table if not exists hive_catalog.default.over_write_tbl2(
        |id int,
        |name string,
        |loc string
        |)using iceberg
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into hive_catalog.default.over_write_tbl2
        |values
        |(1,'rison','beijing'),
        |(2,'zhangsan', 'guangzhou'),
        |(3, 'lisi', 'shagnhai')
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.over_write_tbl2").show()
    //t3 测试表
    spark.sql(
      """
        |create table if not exists hive_catalog.default.over_write_tbl3(
        |id int,
        |name string,
        |loc string
        |)using iceberg
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into hive_catalog.default.over_write_tbl3
        |values
        |(1,'rison','addr'),
        |(3, 'lisi', 'addr'),
        |(2,'zhangsan_new', 'guangzhou')
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.over_write_tbl3").show()

    //TODO insert overwrite t3 到 t2
    spark.sql(
      """
        |insert overwrite hive_catalog.default.over_write_tbl2
        |select * from hive_catalog.default.over_write_tbl3
        |""".stripMargin)

    spark.sql("select * from hive_catalog.default.over_write_tbl2").show()
    //TODO insert overwrite 动态分区 t3 到 t1
    spark.sql(
      """
        |insert overwrite hive_catalog.default.over_write_tbl
        |select * from hive_catalog.default.over_write_tbl3 order by(loc)
        |""".stripMargin)
    spark.sql("select * from hive_catalog.default.over_write_tbl").show()

    //TODO insert overwrite 静态分区 t3 到 t1 (这里t3就不能查询分区列了)
    spark.sql(
      """
        |insert overwrite hive_catalog.default.over_write_tbl
        |partition (loc = 'static_pt')
        |select id, name from hive_catalog.default.over_write_tbl3
        |""".stripMargin)
    spark.sql("select * from hive_catalog.default.over_write_tbl").show()


    spark.close()
  }
}
/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkInsertOverWriteApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */