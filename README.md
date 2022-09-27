# bigdata-iceberg Spark和Flink操作Iceberg

## 1. Spark 操作 Iceberg
### 1.1 前言 spark 和 iceberg 版本信息
* spark 3.1.2
* iceberg 0.12.1
* hive 3.1.2
* hadoop 3.2.1

### 1.2 Spark设置catalog
* hive catalog
```java
spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hive_prod.type = hive
spark.sql.catalog.hive_prod.uri = thrift://metastore-host:port
```
* hadoop catalog
```java
spark.sql.catalog.hadoop_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_prod.type = hadoop
spark.sql.catalog.hadoop_prod.warehouse = hdfs://nn:8020/warehouse/path
```
----
Both catalogs are configured using properties nested under the catalog name. Common configuration properties for Hive and Hadoop are:

| Property |	Values |	Description |
|:-------|:--------|:-----|
|spark.sql.catalog.catalog-name.type | hive or hadoop	|The underlying Iceberg catalog implementation, HiveCatalog, HadoopCatalog or left unset if using a custom catalog
|spark.sql.catalog.catalog-name.catalog-impl	|	| The underlying Iceberg catalog implementation.|
|spark.sql.catalog.catalog-name.default-namespace|	default	|The default current namespace for the catalog|
|spark.sql.catalog.catalog-name.uri|	thrift://host:port	|Metastore connect URI; default from hive-site.xml|
|spark.sql.catalog.catalog-name.warehouse|	hdfs://nn:8020/warehouse/path|	Base path for the warehouse directory|
|spark.sql.catalog.catalog-name.cache-enabled|	true or false	|Whether to enable catalog cache, default value is true|

### 1.3 Spark与Iceberg DDL整合
#### 1.3.1  CREATE TABLE 创建表
**create table 创建Iceberg表，创建表不仅仅可以创建普通表，还可以创建分区表，再向分区表中插入一批数据时，必须要对数据中的分区进行排序，否则会出现文件关闭的错误**
```scala
    //创建分区表，以loc列分区字段
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

```

```shell script
[root@tbds-192-168-0-37 spark-dir]# hdfs dfs -ls hdfs://hdfsCluster/apps/hive/warehouse/partition_tbl
Found 2 items
drwxrwxrwx   - root hadoop          0 2022-09-27 00:28 hdfs://hdfsCluster/apps/hive/warehouse/partition_tbl/data
drwxrwxrwx   - root hadoop          0 2022-09-27 00:28 hdfs://hdfsCluster/apps/hive/warehouse/partition_tbl/metadata
[root@tbds-192-168-0-37 spark-dir]# hdfs dfs -ls hdfs://hdfsCluster/apps/hive/warehouse/partition_tbl/data
Found 2 items
drwxrwxrwx   - root hadoop          0 2022-09-27 00:27 hdfs://hdfsCluster/apps/hive/warehouse/partition_tbl/data/loc=beijing
drwxrwxrwx   - root hadoop          0 2022-09-27 00:28 hdfs://hdfsCluster/apps/hive/warehouse/partition_tbl/data/loc=shanghai

```
**创建Iceberg分区时，还可以通过一些转换表达式对timestamp列来进行转换，创建 ，常用隐藏分区的转换表达式有如下几种：**
* years(ts):按照年分区
```scala
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
        |(2,'zhangsan',19, cast(1603096230  as timestamp)),
        |(3,'lisi',14 ,cast(1608279630  as timestamp)),
        |(4,'wangwu',33,cast(1608279630  as timestamp)),
        |(5,'wangfan',18 ,cast(1634559630  as timestamp)),
        |(6,'liuyi',12 ,cast(1576843830  as timestamp))
        |""".stripMargin)

    spark.sql(
      """
        |select * from hive_catalog.default.partition_year_tbl;
        |""".stripMargin
    )
```
```shell script
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/partition_year_tbl
Found 2 items
drwxrwxrwx   - root hadoop          0 2022-09-27 09:37 /apps/hive/warehouse/partition_year_tbl/data
drwxrwxrwx   - root hadoop          0 2022-09-27 09:37 /apps/hive/warehouse/partition_year_tbl/metadata
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/partition_year_tbl/data
Found 3 items
drwxrwxrwx   - root hadoop          0 2022-09-27 09:37 /apps/hive/warehouse/partition_year_tbl/data/ts_year=2019
drwxrwxrwx   - root hadoop          0 2022-09-27 09:37 /apps/hive/warehouse/partition_year_tbl/data/ts_year=2020
drwxrwxrwx   - root hadoop          0 2022-09-27 09:37 /apps/hive/warehouse/partition_year_tbl/data/ts_year=2021
[root@tbds-192-168-0-37 ~]# 

```
注意：向表插入数据，必须要按照年来排序，只要相同的年份写在一起就可以

* months(ts):按照“年-月”月级别分区
```scala
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
```

```shell script
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/partition_month_tbl
Found 2 items
drwxrwxrwx   - root hadoop          0 2022-09-27 09:43 /apps/hive/warehouse/partition_month_tbl/data
drwxrwxrwx   - root hadoop          0 2022-09-27 09:43 /apps/hive/warehouse/partition_month_tbl/metadata
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/partition_month_tbl/data
Found 5 items
drwxrwxrwx   - root hadoop          0 2022-09-27 09:43 /apps/hive/warehouse/partition_month_tbl/data/ts_month=2019-12
drwxrwxrwx   - root hadoop          0 2022-09-27 09:43 /apps/hive/warehouse/partition_month_tbl/data/ts_month=2020-10
drwxrwxrwx   - root hadoop          0 2022-09-27 09:43 /apps/hive/warehouse/partition_month_tbl/data/ts_month=2020-12
drwxrwxrwx   - root hadoop          0 2022-09-27 09:43 /apps/hive/warehouse/partition_month_tbl/data/ts_month=2021-10
drwxrwxrwx   - root hadoop          0 2022-09-27 09:43 /apps/hive/warehouse/partition_month_tbl/data/ts_month=2021-12

```

* days(ts)或者date(ts):按照“年-月-日”天级别分区

```scala
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
```

```shell script
+---+--------+---+-------------------+
| id|    name|age|                 ts|
+---+--------+---+-------------------+
|  1|   rison| 18|2020-12-20 21:10:30|
|  2|zhangsan| 19|2020-12-18 16:20:30|
|  3|    lisi| 14|2021-10-18 20:20:30|
|  4|  wangwu| 33|2020-10-19 16:30:30|
|  5| wangfan| 18|2021-12-19 21:30:30|
|  6|   liuyi| 12|2019-12-20 20:10:30|
+---+--------+---+-------------------+

[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/partition_day_tbl
Found 2 items
drwxrwxrwx   - root hadoop          0 2022-09-27 09:50 /apps/hive/warehouse/partition_day_tbl/data
drwxrwxrwx   - root hadoop          0 2022-09-27 09:50 /apps/hive/warehouse/partition_day_tbl/metadata
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/partition_day_tbl/data
Found 11 items
drwxrwxrwx   - root hadoop          0 2022-09-27 09:50 /apps/hive/warehouse/partition_day_tbl/data/ts_day=2019-12-20
drwxrwxrwx   - root hadoop          0 2022-09-27 09:50 /apps/hive/warehouse/partition_day_tbl/data/ts_day=2020-10-19
drwxrwxrwx   - root hadoop          0 2022-09-27 09:50 /apps/hive/warehouse/partition_day_tbl/data/ts_day=2020-12-18
drwxrwxrwx   - root hadoop          0 2022-09-27 09:50 /apps/hive/warehouse/partition_day_tbl/data/ts_day=2020-12-20
drwxrwxrwx   - root hadoop          0 2022-09-27 09:50 /apps/hive/warehouse/partition_day_tbl/data/ts_day=2021-10-18
drwxrwxrwx   - root hadoop          0 2022-09-27 09:50 /apps/hive/warehouse/partition_day_tbl/data/ts_day=2021-12-19

```
* hours(ts)或者date_hour(ts):按照“年-月-日-时”小时级别分区
```
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
```
```shell script
+---+--------+---+-------------------+
| id|    name|age|                 ts|
+---+--------+---+-------------------+
|  1|   rison| 18|2020-12-20 21:10:30|
|  2|zhangsan| 19|2020-12-18 16:20:30|
|  3|    lisi| 14|2021-10-18 20:20:30|
|  4|  wangwu| 33|2020-10-19 16:30:30|
|  5| wangfan| 18|2021-12-19 21:30:30|
|  6|   liuyi| 12|2019-12-20 20:10:30|
+---+--------+---+-------------------+
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/partition_hour_tbl
Found 2 items
drwxrwxrwx   - root hadoop          0 2022-09-27 09:53 /apps/hive/warehouse/partition_hour_tbl/data
drwxrwxrwx   - root hadoop          0 2022-09-27 09:54 /apps/hive/warehouse/partition_hour_tbl/metadata
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/partition_hour_tbl/data
Found 6 items
drwxrwxrwx   - root hadoop          0 2022-09-27 09:53 /apps/hive/warehouse/partition_hour_tbl/data/ts_hour=2019-12-20-12
drwxrwxrwx   - root hadoop          0 2022-09-27 09:53 /apps/hive/warehouse/partition_hour_tbl/data/ts_hour=2020-10-19-08
drwxrwxrwx   - root hadoop          0 2022-09-27 09:53 /apps/hive/warehouse/partition_hour_tbl/data/ts_hour=2020-12-18-08
drwxrwxrwx   - root hadoop          0 2022-09-27 09:53 /apps/hive/warehouse/partition_hour_tbl/data/ts_hour=2020-12-20-13
drwxrwxrwx   - root hadoop          0 2022-09-27 09:53 /apps/hive/warehouse/partition_hour_tbl/data/ts_hour=2021-10-18-12
drwxrwxrwx   - root hadoop          0 2022-09-27 09:53 /apps/hive/warehouse/partition_hour_tbl/data/ts_hour=2021-12-19-13

```
备注：Iceberg支持的时间分区目前和将来只支持UTC,UTC是国际时，UTC+8就是国际时加八小时，是东八区时间,也就是北京时间，所以我们看到上面分区时间与数据时间不一致。但是查询不影响自动转换。
除了以上常用的时间隐藏分区外，Iceberg还支持bucket(N,col)分区，这种分区方式可以按照某列的hash值与N取余决定数据去往的分区。truncate(L,col)，这种隐藏分区可以将字符串列截取L长度，相同的数据会被分到相同分区中。

**Partition Transforms**

|Transform name|Description	|Source types|	Result type|
|:-------|:--------|:-----|:---------|
|identity|	Source value, unmodified|	Any	|Source type|
|bucket[N]|	Hash of value, mod N (see below)|	int, long, decimal, date, time, timestamp, timestamptz, string, uuid, fixed, binary|	int|
|truncate[W]|	Value truncated to width W (see below)|	int, long, decimal, string	|Source type|
|year|	Extract a date or timestamp year, as years from 1970|	date, timestamp, timestamptz|	int|
|month|	Extract a date or timestamp month, as months from 1970-01-01|	date, timestamp, timestamptz|	int|
|day|	Extract a date or timestamp day, as days from 1970-01-01 |date, timestamp, timestamptz|	date|
|hour|	Extract a timestamp hour, as hours from 1970-01-01 00:00:00	timestamp, timestamptz|	int|
|void|	Always produces null|	Any	|Source type or int|

#### 1.3.2 CREATE TABLE ...  AS SELECT
Iceberg 支持 ‘**create table ... as select**’语法，可以从查询语句中创建一张表，并插入对应的数据

```scala
   spark.sql(
        """
          |create table hive_catalog.default.as_select_tbl using iceberg as select id, name, age from hive_catalog.default.normal_tbl
          |""".stripMargin)
  
      spark.sql(
        """
          |select * from  hive_catalog.default.as_select_tbl
          |""".stripMargin
      ).show()
```

```shell script
+---+-----+---+
| id| name|age|
+---+-----+---+
|  1|rison| 18|
|  1|rison| 18|
+---+-----+---+
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/as_select_tbl/data
Found 2 items
-rw-r--r--   3 root hadoop       1050 2022-09-27 10:39 /apps/hive/warehouse/as_select_tbl/data/00000-16-61e55873-abc9-4c99-8bed-7b527e9cba40-00001.parquet
-rw-r--r--   3 root hadoop        889 2022-09-27 10:42 /apps/hive/warehouse/as_select_tbl/data/00000-16-65e7bfb1-5898-47f0-911d-853a32ca88ec-00001.parquet
[root@tbds-192-168-0-37 ~]# 

```
#### 1.3.3 DROP TABLE 删表
删除表时，目录依然存在，但是data目录下的数据文件被删除了。
```scala
    spark.sql(
      """
        |drop table hive_catalog.default.normal_tbl
        |""".stripMargin
    )

```
```
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/normal_tbl/
Found 2 items
drwxrwxrwx   - root hadoop          0 2022-09-27 10:42 /apps/hive/warehouse/normal_tbl/data
drwxrwxrwx   - root hadoop          0 2022-09-27 10:42 /apps/hive/warehouse/normal_tbl/metadata
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/normal_tbl/data
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/normal_tbl/metadata
Found 4 items
-rw-r--r--   3 root hadoop       1372 2022-09-27 00:27 /apps/hive/warehouse/normal_tbl/metadata/00000-88998725-3f32-4d3b-ad10-06d8b2c1a4ec.metadata.json
-rw-r--r--   3 root hadoop       1372 2022-09-27 10:37 /apps/hive/warehouse/normal_tbl/metadata/00000-cf525b63-ed0d-4abb-b791-d285113c3876.metadata.json
-rw-r--r--   3 root hadoop       1372 2022-09-27 10:42 /apps/hive/warehouse/normal_tbl/metadata/00000-df84e452-47eb-4fa0-926d-c1175bf33269.metadata.json
-rw-r--r--   3 root hadoop       2355 2022-09-27 10:37 /apps/hive/warehouse/normal_tbl/metadata/00001-716da976-1762-4ae4-8d3a-2d9828eb0870.metadata.json
[root@tbds-192-168-0-37 ~]# 
```

#### 1.3.3 ALTER TABLE 修改表
Iceberg的 alter 操作在Spark3.x版本中支持，alter一般包含如下操作：
* 添加、删除列
**添加列：ALTER TABLE ... ADD COLUMN**
**删除列：ALTER TABLE ... DROP COLUMN**
```scala
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
```
```
## 原始表
+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  2|zhagnsan| 20|
+---+--------+---+
## 添加 gender/loc 列
+---+--------+---+------+----+
| id|    name|age|gender| loc|
+---+--------+---+------+----+
|  1|   rison| 18|  null|null|
|  2|zhagnsan| 20|  null|null|
+---+--------+---+------+----+
## 删除age列
+---+--------+------+----+
| id|    name|gender| loc|
+---+--------+------+----+
|  1|   rison|  null|null|
|  2|zhagnsan|  null|null|
+---+--------+------+----+

```
* 重命名列
**重命名列：ALTER TABLE ... RENAME COLUMN**

```scala
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
```

```
## 原始表
+---+--------+----+
| id|    name| age|
+---+--------+----+
|  1|   rison|null|
|  2|zhagnsan|null|
|  1|   rison|  18|
|  2|zhagnsan|  20|
|  1|   rison|null|
|  2|zhagnsan|null|
+---+--------+----+
## 修改id为id_rename
+---------+--------+----+
|id_rename|    name| age|
+---------+--------+----+
|        1|   rison|null|
|        2|zhagnsan|null|
|        1|   rison|null|
|        2|zhagnsan|null|
|        1|   rison|null|
|        2|zhagnsan|null|
+---------+--------+----+

```
#### 1.3.4 ALTER TABLE 修改分区
alter修改分区，包括添加分区和删除分区，这种分区操作在spark3.x之后被支持，
使用之前必须要添加spark.sql.extensions属性，其值为：org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
在添加分区时还支持分区转换，语法如下：
* 添加分区：ALTER TABLE...ADD PARTITION FIELD
```scala
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
        |(11,'rison_loc','beijing',cast(1639920630 as timestamp)),
        |(22,'zhangsan_loc','guangzhou',cast(1576843830 as timestamp))
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
        |(111,'rison_ts','beijing',cast(1639920630 as timestamp)),
        |(222,'zhangsan_ts','guangzhou',cast(1576843830 as timestamp))
        |""".stripMargin
    )
    spark.sql(
      """
        |select * from hive_catalog.default.alter_partition_tbl
        |""".stripMargin
    ).show()

```

```
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/alter_partition_tbl
Found 2 items
drwxrwxrwx   - root hadoop          0 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data
drwxrwxrwx   - root hadoop          0 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/metadata
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/alter_partition_tbl/data
Found 6 items
-rw-r--r--   3 root hadoop       1172 2022-09-27 12:57 /apps/hive/warehouse/alter_partition_tbl/data/00000-23-4aea357a-f81a-4ba6-92c2-ca440dc36864-00001.parquet
-rw-r--r--   3 root hadoop       1172 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/00000-23-e8b8f0ab-9843-4d88-80b2-91fad43fa001-00001.parquet
-rw-r--r--   3 root hadoop       1207 2022-09-27 12:57 /apps/hive/warehouse/alter_partition_tbl/data/00001-24-2c0f2673-66b2-4c1d-9e08-93afe8e8677e-00001.parquet
-rw-r--r--   3 root hadoop       1207 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/00001-24-a0afdc71-1f3f-4ecb-8fa2-53767c26bcea-00001.parquet
drwxrwxrwx   - root hadoop          0 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/loc=beijing
drwxrwxrwx   - root hadoop          0 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/loc=guangzhou
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/alter_partition_tbl/data/loc=beijing
Found 3 items
-rw-r--r--   3 root hadoop       1172 2022-09-27 13:00 /apps/hive/warehouse/alter_partition_tbl/data/loc=beijing/00000-23-dd00833b-be14-4514-90c0-73d2a8f75776-00001.parquet
-rw-r--r--   3 root hadoop       1199 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/loc=beijing/00000-25-5502165c-3873-4b19-9c18-fede5f894c19-00001.parquet
drwxrwxrwx   - root hadoop          0 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/loc=beijing/ts_year=2021
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/alter_partition_tbl/data/loc=beijing/ts_year=2021
Found 1 items
-rw-r--r--   3 root hadoop       1185 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/loc=beijing/ts_year=2021/00000-28-fb9efdb4-14e6-49af-8f99-201a81ff3465-00001.parquet

```
添加分区字段是元数据操作，不会改表现有的表数据，新的数据将使用新分区写入数据，现有数据将继续保留在原有的分区布局中。

* 删除分区：ALTER TABLE...DROP PARTITION FIELD

```scala
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
```
```
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/alter_partition_tbl/data/
Found 7 items
-rw-r--r--   3 root hadoop       1172 2022-09-27 12:57 /apps/hive/warehouse/alter_partition_tbl/data/00000-23-4aea357a-f81a-4ba6-92c2-ca440dc36864-00001.parquet
-rw-r--r--   3 root hadoop       1172 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/00000-23-e8b8f0ab-9843-4d88-80b2-91fad43fa001-00001.parquet
-rw-r--r--   3 root hadoop       1207 2022-09-27 12:57 /apps/hive/warehouse/alter_partition_tbl/data/00001-24-2c0f2673-66b2-4c1d-9e08-93afe8e8677e-00001.parquet
-rw-r--r--   3 root hadoop       1207 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/00001-24-a0afdc71-1f3f-4ecb-8fa2-53767c26bcea-00001.parquet
drwxrwxrwx   - root hadoop          0 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/loc=beijing
drwxrwxrwx   - root hadoop          0 2022-09-27 13:03 /apps/hive/warehouse/alter_partition_tbl/data/loc=guangzhou
drwxrwxrwx   - root hadoop          0 2022-09-27 13:11 /apps/hive/warehouse/alter_partition_tbl/data/loc=null
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/alter_partition_tbl/data/loc=null
Found 1 items
drwxrwxrwx   - root hadoop          0 2022-09-27 13:11 /apps/hive/warehouse/alter_partition_tbl/data/loc=null/ts_year=null
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/alter_partition_tbl/data/loc=null/ts_year=null
Found 2 items
-rw-r--r--   3 root hadoop       1200 2022-09-27 13:11 /apps/hive/warehouse/alter_partition_tbl/data/loc=null/ts_year=null/00000-23-7737266b-4cfe-40b5-a52a-ca5eceb57e16-00001.parquet
-rw-r--r--   3 root hadoop       1242 2022-09-27 13:11 /apps/hive/warehouse/alter_partition_tbl/data/loc=null/ts_year=null/00001-24-b14e017d-61e1-4fb0-b635-bc5e48cf21c8-00001.parquet
[root@tbds-192-168-0-37 ~]# 

```
我们发现，删除表的loc分区、years(ts)分区之后,目录变成**loc=null/ts_year=null**，后面的新数据将保存在该路径下。

### 1.4 DataFrame API加载Iceberg中的数据
Spark 操作Iceberg 不仅可以通过SQL的方式查询Iceberg数据，还可以使用dataFrame的方式加载到Iceberg表中，
可以通过spark.table(表名)或者spark.read.format(iceberg).load(iceberg data path)来加载对应的表数据：
```scala
  spark.sql(
      """
        |create table if not exists  hive_catalog.default.iceberg_test_tbl(id int, name string, age int) using iceberg
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
    // 1.1 sql的方式读取iceberg的数据
    spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()

    // 1.2 dataframe的方式读取iceberg的数据
    spark.table("hive_catalog.default.iceberg_test_tbl").show()
    spark.read.format("iceberg")
      .load("hdfs://hdfsCluster/apps/hive/warehouse/iceberg_test_tbl").show()

```










## 2. Flink 操作 Iceberg