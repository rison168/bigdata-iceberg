package com.rison.bigdata

import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkDDLApplication
 * @USER: Rison
 * @DATE: 2022/9/27 0:15
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkDDLApplication {
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


    //TODO 1. 创建普通表
    spark.sql(
      """
        |create table if not exists hive_catalog.default.normal_tbl
        |(id int,
        |name string,
        |age int
        |) using iceberg
        |""".stripMargin
    )

    spark.sql(
      """
        |insert into table hive_catalog.default.normal_tbl values (1,'rison',18)
        |""".stripMargin
    )

    //TODO 2. 创建分区表，以loc列分区字段
    spark.sql(
      """
        |create table if not exists hive_catalog.default.partition_tbl
        |(id int,
        |name string,
        |age int,
        |loc string
        |) using iceberg
        |partitioned by (loc)
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.partition_tbl
        |values
        |(1,'rison',16,'beijing'),
        |(2,'zhangsan',18,'beijing'),
        |(3,'lisi',18,'shanghai')
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from hive_catalog.default.partition_tbl
        |""".stripMargin).show()

    //TODO 3. 创建years(ts):按照年分区表
    spark.sql(
      """
        |create table if not exists  hive_catalog.default.partition_year_tbl(
        |id int,
        |name string,
        |age int,
        |ts timestamp
        |)using iceberg
        |partitioned by (years(ts))
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.partition_year_tbl
        |values
        |(1,'rison',18, cast(1608469830 as timestamp)),
        |(2,'zhangsan',19,cast(1603096230  as timestamp)),
        |(3,'lisi',14 ,cast(1608279630  as timestamp)),
        |(4,'wangwu',33,cast(1608279630  as timestamp)),
        |(5,'wangfan',18 ,cast(1634559630  as timestamp)),
        |(6,'liuyi',12 ,cast(1576843830  as timestamp))
        |""".stripMargin)

    spark.sql(
      """
        |select * from hive_catalog.default.partition_year_tbl;
        |""".stripMargin
    ).show

    //TODO 4. months(ts):按照“年-月”月级别分区
    spark.sql(
      """
        |create table if not exists  hive_catalog.default.partition_month_tbl(
        |id int,
        |name string,
        |age int,
        |ts timestamp
        |)using iceberg
        |partitioned by (months(ts))
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.partition_month_tbl
        |values
        |(1,'rison',18, cast(1608469830 as timestamp)),
        |(2,'zhangsan',19,cast(1608279630 as timestamp)),
        |(3,'lisi',14 ,cast(1634559630 as timestamp)),
        |(4,'wangwu',33,cast(1603096230  as timestamp)),
        |(5,'wangfan',18 ,cast(1639920630 as timestamp)),
        |(6,'liuyi',12 ,cast(1576843830 as timestamp))
        |""".stripMargin)

    spark.sql(
      """
        |select * from hive_catalog.default.partition_month_tbl;
        |""".stripMargin
    ).show

    //TODO 5. days(ts)或者date(ts):按照“年-月-日”天级别分区
    spark.sql(
      """
        |create table if not exists  hive_catalog.default.partition_day_tbl(
        |id int,
        |name string,
        |age int,
        |ts timestamp
        |)using iceberg
        |partitioned by (days(ts))
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.partition_day_tbl
        |values
        |(1,'rison',18, cast(1608469830  as timestamp)),
        |(2,'zhangsan',19,cast(1608279630 as timestamp)),
        |(3,'lisi',14 ,cast(1634559630 as timestamp)),
        |(4,'wangwu',33,cast(1603096230  as timestamp)),
        |(5,'wangfan',18 ,cast(1639920630 as timestamp)),
        |(6,'liuyi',12 ,cast(1576843830 as timestamp))
        |""".stripMargin)

    spark.sql(
      """
        |select * from hive_catalog.default.partition_day_tbl;
        |""".stripMargin
    ).show


    //TODO 5. days(ts)或者date(ts):按照“年-月-日”天级别分区
    spark.sql(
      """
        |create table if not exists  hive_catalog.default.partition_hour_tbl(
        |id int,
        |name string,
        |age int,
        |ts timestamp
        |)using iceberg
        |partitioned by (hours(ts))
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.partition_hour_tbl
        |values
        |(1,'rison',18, cast(1608469830  as timestamp)),
        |(2,'zhangsan',19,cast(1608279630 as timestamp)),
        |(3,'lisi',14 ,cast(1634559630 as timestamp)),
        |(4,'wangwu',33,cast(1603096230  as timestamp)),
        |(5,'wangfan',18 ,cast(1639920630 as timestamp)),
        |(6,'liuyi',12 ,cast(1576843830 as timestamp))
        |""".stripMargin)

    spark.sql(
      """
        |select * from hive_catalog.default.partition_hour_tbl;
        |""".stripMargin
    ).show

    //TODO 6. CREATE TAEBL ...  AS SELECT 创建表并插入数据
    spark.sql(
      """
        |create table if not exists hive_catalog.default.as_select_tbl using iceberg as select id, name, age from hive_catalog.default.normal_tbl
        |""".stripMargin)

    spark.sql(
      """
        |select * from  hive_catalog.default.as_select_tbl
        |""".stripMargin
    ).show()

    //TODO 7. DROP TABLE 删除表
    spark.sql(
      """
        |drop table hive_catalog.default.normal_tbl
        |""".stripMargin
    )

    //TODO 8. ALTER TABLE 修改表
    spark.sql(
      """
        |create table if not exists hive_catalog.default.alter_tbl
        |(id int,
        |name string,
        |age int
        |) using iceberg
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.alter_tbl values (1,'rison',18),(2,'zhagnsan',20)
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from  hive_catalog.default.alter_tbl
        |""".stripMargin
    ).show()
    //添加列
    spark.sql(
      """
        |alter table hive_catalog.default.alter_tbl add column gender string,loc string
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from  hive_catalog.default.alter_tbl
        |""".stripMargin
    ).show()
    //删除列
    spark.sql(
      """
        |alter table hive_catalog.default.alter_tbl drop column age
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from  hive_catalog.default.alter_tbl
        |""".stripMargin
    ).show()
    spark.sql(
      """
        |alter table hive_catalog.default.alter_tbl drop column gender,loc
        |""".stripMargin
    )
    spark.sql(
      """
        |alter table hive_catalog.default.alter_tbl add column age int
        |""".stripMargin
    )
    //重命名列
    spark.sql(
      """
        |alter table hive_catalog.default.alter_tbl rename column id to id_rename
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from  hive_catalog.default.alter_tbl
        |""".stripMargin
    ).show()
    spark.sql(
      """
        |alter table hive_catalog.default.alter_tbl rename column id_rename to id
        |""".stripMargin
    )
    //TODO 9. ALTER TABLE 修改分区
    //创建分区表

    spark.sql(
      """
        |create table if not exists hive_catalog.default.alter_partition_tbl
        |(id int,
        |name string,
        |loc string,
        |ts timestamp
        |) using iceberg
        |
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.alter_partition_tbl
        |values
        |(1,'rison','beijing',cast(1639920630 as timestamp)),
        |(2,'zhangsan','guangzhou',cast(1576843830 as timestamp))
        |""".stripMargin
    )
    //添加loc为分区
    spark.sql(
      """
        |alter table hive_catalog.default.alter_partition_tbl add partition field loc
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.alter_partition_tbl
        |values
        |(11,'rison-loc','beijing',cast(1639920630 as timestamp)),
        |(21,'zhangsan-loc','guangzhou',cast(1576843830 as timestamp))
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from hive_catalog.default.alter_partition_tbl
        |""".stripMargin
    ).show()
    //添加years(ts)为分区
    spark.sql(
      """
        |alter table hive_catalog.default.alter_partition_tbl add partition field years(ts)
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.alter_partition_tbl
        |values
        |(111,'riso-ts','beijing',cast(1639920630 as timestamp)),
        |(222,'zhangsan-ts','guangzhou',cast(1576843830 as timestamp))
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from hive_catalog.default.alter_partition_tbl
        |""".stripMargin
    ).show()

    //删除分区
    spark.sql(
      """
        |alter table hive_catalog.default.alter_partition_tbl drop partition field years(ts)
        |""".stripMargin
    )
    spark.sql(
      """
        |alter table hive_catalog.default.alter_partition_tbl drop partition field loc
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.alter_partition_tbl
        |values
        |(1111,'riso-drop','beijing',cast(1639920630 as timestamp)),
        |(2222,'zhangsan-drop','guangzhou',cast(1576843830 as timestamp))
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from hive_catalog.default.alter_partition_tbl
        |""".stripMargin
    ).show()





    spark.close();
  }
}

/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkDDLApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */