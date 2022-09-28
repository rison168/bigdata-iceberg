package com.rison.bigdata

import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkInsertApplication
 * @USER: Rison
 * @DATE: 2022/9/28 11:08
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkMergeIntoApplication {
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

    //TODO 创建表a、表b ,插入数据

    //创建表a
    spark.sql("drop table  if exists hive_catalog.default.a")
    spark.sql(
      """
        |create table if not exists  hive_catalog.default.a(
        |id int,
        |name string,
        |age int
        |) using iceberg
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into hive_catalog.default.a values(1, 'rison', 18),(2, 'zhangsan',20),(3, 'lisi', 22)
        |""".stripMargin)
    spark.sql("select * from hive_catalog.default.a").show()
    //创建表b
    spark.sql("drop table if exists hive_catalog.default.b")
    spark.sql(
      """
        |create table if not exists hive_catalog.default.b(
        |id int,
        |name string,
        |age int,
        |op string
        |) using iceberg
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into hive_catalog.default.b values(1, 'rison', 18, 'D'),(2, 'zhangsan_new',100, 'U'),(4, 'new boy1', 22, 'I'),(5, 'new boy2', 22, 'I')
        |""".stripMargin)
    spark.sql("select * from hive_catalog.default.b").show()

    //TODO MERGE INTO 向source表更新、删除、新增数据
    spark.sql(
      """
        |merge into hive_catalog.default.a t1
        |using (select id, name, age, op from hive_catalog.default.b) t2
        |on t1.id = t2.id
        |when matched and t2.op = 'D' then delete
        |when matched and t2.op = 'U' then update set t1.name = t2.name, t1.age = t2.age
        |when not matched then insert (id, name, age) values (t2.id, t2.name, t2.age)
        |
        |""".stripMargin
    )

    spark.sql("select * from hive_catalog.default.a").show()

    spark.close()
  }
}
/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkMergeIntoApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */