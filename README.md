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
### 1.5 Iceberg 查询快照
每次向iceberg表中commit数据都会生成对应的一个快照信息
我们可以通过查询catalog.db.table.snapshots 来查询iceberg表中拥有的快照，操作如下：

```scala
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
        |select * from hive_catalog.default.iceberg_test_tbl.snapshots
        |""".stripMargin
    ).show()

```

```shell script
22/09/27 14:13:49 INFO CodeGenerator: Code generated in 24.651702 ms
+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
+---+--------+---+
+--------------------+-------------------+-------------------+---------+--------------------+--------------------+
|        committed_at|        snapshot_id|          parent_id|operation|       manifest_list|             summary|
+--------------------+-------------------+-------------------+---------+--------------------+--------------------+
|2022-09-27 13:52:...| 609321932124834691|               null|   append|hdfs://hdfsCluste...|{spark.app.id -> ...|
|2022-09-27 13:57:...|1368006528896806597| 609321932124834691|   append|hdfs://hdfsCluste...|{spark.app.id -> ...|
|2022-09-27 14:10:...|1665322165591746063|1368006528896806597|   append|hdfs://hdfsCluste...|{spark.app.id -> ...|
|2022-09-27 14:13:...|8961166628509057021|1665322165591746063|   append|hdfs://hdfsCluste...|{spark.app.id -> ...|
+--------------------+-------------------+-------------------+---------+--------------------+--------------------+

```
### 1.6 Iceberg 查询表历史

我们可以通过查询catalog.db.table.history 来查询iceberg表的历史信息（表快照信息内容），操作如下：
```scala
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
```
```shell script
22/09/27 14:19:52 INFO CodeGenerator: Code generated in 19.081558 ms
+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
+---+--------+---+
+--------------------+-------------------+-------------------+-------------------+
|     made_current_at|        snapshot_id|          parent_id|is_current_ancestor|
+--------------------+-------------------+-------------------+-------------------+
|2022-09-27 13:52:...| 609321932124834691|               null|               true|
|2022-09-27 13:57:...|1368006528896806597| 609321932124834691|               true|
|2022-09-27 14:10:...|1665322165591746063|1368006528896806597|               true|
|2022-09-27 14:13:...|8961166628509057021|1665322165591746063|               true|
|2022-09-27 14:19:...|8667987842706378050|8961166628509057021|               true|
+--------------------+-------------------+-------------------+-------------------+

```
### 1.7 Iceberg 查询表 data files
{catalog}.{database}.{table}.files
```scala
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
        |select * from hive_catalog.default.iceberg_test_tbl.files
        |""".stripMargin
    ).show()

```
```shell script

22/09/27 14:27:24 INFO CodeGenerator: Code generated in 29.642955 ms
+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
+---+--------+---+

+-------+--------------------+-----------+------------+------------------+--------------------+--------------------+--------------------+----------------+--------------------+--------------------+------------+-------------+------------+-------------+
|content|           file_path|file_format|record_count|file_size_in_bytes|        column_sizes|        value_counts|   null_value_counts|nan_value_counts|        lower_bounds|        upper_bounds|key_metadata|split_offsets|equality_ids|sort_order_id|
+-------+--------------------+-----------+------------+------------------+--------------------+--------------------+--------------------+----------------+--------------------+--------------------+------------+-------------+------------+-------------+
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               871|{1 -> 47, 2 -> 52...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               892|{1 -> 47, 2 -> 55...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               871|{1 -> 47, 2 -> 52...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               892|{1 -> 47, 2 -> 55...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               871|{1 -> 47, 2 -> 52...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               892|{1 -> 47, 2 -> 55...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               871|{1 -> 47, 2 -> 52...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               892|{1 -> 47, 2 -> 55...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               871|{1 -> 47, 2 -> 52...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               892|{1 -> 47, 2 -> 55...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               871|{1 -> 47, 2 -> 52...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               892|{1 -> 47, 2 -> 55...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               871|{1 -> 47, 2 -> 52...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
|      0|hdfs://hdfsCluste...|    PARQUET|           1|               892|{1 -> 47, 2 -> 55...|{1 -> 1, 2 -> 1, ...|{1 -> 0, 2 -> 0, ...|              {}|{1 -> , 2 -> ...|{1 -> , 2 -> ...|        null|          [4]|        null|            0|
+-------+--------------------+-----------+------------+------------------+--------------------+--------------------+--------------------+----------------+--------------------+--------------------+------------+-------------+------------+-------------+

```

### 1.8 Iceberg 查询表 manifests
{catalog}.{database}.{table}.manifests

```scala
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
        |select * from hive_catalog.default.iceberg_test_tbl.manifests
        |""".stripMargin
    ).show()

```

```shell script
22/09/27 14:55:39 INFO CodeGenerator: Code generated in 30.500539 ms
+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  1|   rison| 18|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  2|zhangsan| 20|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
+---+--------+---+
+--------------------+------+-----------------+-------------------+----------------------+-------------------------+------------------------+-------------------+
|                path|length|partition_spec_id|  added_snapshot_id|added_data_files_count|existing_data_files_count|deleted_data_files_count|partition_summaries|
+--------------------+------+-----------------+-------------------+----------------------+-------------------------+------------------------+-------------------+
|hdfs://hdfsCluste...|  5930|                0|3268582405449064443|                     2|                        0|                       0|                 []|
|hdfs://hdfsCluste...|  5930|                0|3210846780360171248|                     2|                        0|                       0|                 []|
|hdfs://hdfsCluste...|  5929|                0|4682874639393672542|                     2|                        0|                       0|                 []|
|hdfs://hdfsCluste...|  5929|                0|8667987842706378050|                     2|                        0|                       0|                 []|
|hdfs://hdfsCluste...|  5928|                0|8961166628509057021|                     2|                        0|                       0|                 []|
|hdfs://hdfsCluste...|  5927|                0|1665322165591746063|                     2|                        0|                       0|                 []|
|hdfs://hdfsCluste...|  5929|                0|1368006528896806597|                     2|                        0|                       0|                 []|
|hdfs://hdfsCluste...|  5927|                0| 609321932124834691|                     2|                        0|                       0|                 []|
+--------------------+------+-----------------+-------------------+----------------------+-------------------------+------------------------+-------------------+

```
### 1.9 Iceberg 查询指定表快照数据
查询Iceberg表数据可以指定snapshot-id来查询指定快照的数据，这种方式可以使用
dataFrame api 方式查询，Spark3.x 可以通过sql方式来查询，操作如下：

```scala
        spark.sql(
          """
            |create table if not exists hive_catalog.default.iceberg_test_tbl(id int, name string, age int) using iceberg
            |""".stripMargin
        )
        spark.sql(
          """
            |insert into table hive_catalog.default.iceberg_test_tbl
            |values
            |(1,'rison_new', 18),
            |(2, 'zhangsan_new', 20)
            |""".stripMargin
        )
        spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()
    
        //查询指定快照数据，快照ID可以通过读取json元数据的文件获取
        spark.read
          .option("snapshot-id", 3210846780360171248L)
          .format("iceberg")
          .table("hive_catalog.default.iceberg_test_tbl")
          .show()
    
        //spark3.x版本 设定当前快照id,sql查询数据
        spark.sql(
          """
            |call hive_catalog.system.set_current_snapshot('default.iceberg_test_tbl', 3210846780360171248)
            |""".stripMargin
        )
        spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()
```

```
22/09/27 16:45:09 INFO CodeGenerator: Code generated in 30.46259 ms
+---+------------+---+
| id|        name|age|
+---+------------+---+
|  1|   rison_new| 18|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  2|zhangsan_new| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
+---+------------+---+
+---+------------+-
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
+---+--------+---+

+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
+---+--------+---+

```
### 1.10 Iceberg 根据时间戳查询数据
Spark读取Iceberg数据可以指定’as-of-timestamp‘参数，通过指定的一个毫秒时间参数查询iceberg表数据，
iceberg会根据元数据找出timestamp-ms <= as-of-timestamp对应的snapshot-id,
spark3.x支持SQL指定时间查询数据。

```scala
  spark.sql(
      """
        |create table if not exists hive_catalog.default.iceberg_test_tbl(id int, name string, age int) using iceberg
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.iceberg_test_tbl
        |values
        |(1,'rison_new', 18),
        |(2, 'zhangsan_new', 20)
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()
    //dataframe api
    spark.read
      .option("as-of-timestamp", "1664268086000")
      .table("hive_catalog.default.iceberg_test_tbl")
      .show()
    // 回滚设定当前时间戳, sql查询当前数据
    spark.sql(
      """
        |CALL hive_catalog.system.rollback_to_timestamp('default.iceberg_test_tbl', TIMESTAMP '2022-09-27 16:41:26')
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()
```

```shell script
+---+------------+---+
| id|        name|age|
+---+------------+---+
|  1|       rison| 18|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  2|    zhangsan| 20|
|  1|   rison_new| 18|
|  2|zhangsan_new| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
|  1|       rison| 18|
|  2|    zhangsan| 20|
+---+------------+---+
+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
+---+--------+---+
+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
|  1|   rison| 18|
|  2|zhangsan| 20|
+---+--------+---+
```
### 1.11 Iceberg 回滚快照
iceberg可以回滚快照，可以借助Java代码实现， dataframe api 并没提供对应的接口，
spark3.x版本，支持sql回滚，Iceberg对应的表中会生成新的Snapshot-id,重新查询，回滚生效。

```scala
    spark.sql(
      """
        |create table if not exists hive_catalog.default.iceberg_test_tbl(id int, name string, age int) using iceberg
        |""".stripMargin
    )
    spark.sql(
      """
        |insert into table hive_catalog.default.iceberg_test_tbl
        |values
        |(1,'rison_new', 18),
        |(2, 'zhangsan_new', 20)
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()
    //1. java api 方式 回滚快照
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://hdfsCluster")
    conf.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/hdfs-site.xml"))
    conf.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/core-site.xml"))
    conf.addResource(new Path("/usr/hdp/current/hive-client/conf/hive-site.xml"))
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    //hadoop catalog 模式
//    val catalog = new HadoopCatalog(conf, "hdfs://hdfsCluster/apps/hive/warehouse")
    val catalog = new HiveCatalog(conf)
    catalog.setConf(conf)
    val table: Table = catalog.loadTable(TableIdentifier.of("default", "iceberg_test_tbl"))
    table.manageSnapshots().rollbackTo(3210846780360171248L).commit()

    spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()

    //2. spark3.x版本 sql方式回滚快照
    spark.sql(
      """
        |call hive_catalog.system.rollback_to_snapshot('default.iceberg_test_tbl', 3210846780360171248)
        |""".stripMargin
    )
    spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()

```
### 1.12 Iceberg 合并表数据文件

iceberg 表每次commit都会生成一个parquet文件，有可能一张表iceberg表对应的数据文件非常多，
通过Java Api的方式对iceberg表可以进行数据文件合并，数据文件合并之后，会生成新的Snapshot,
原有的数据并不会删除，如果要删除对应的数据文件，需要通过‘Expire snapshots’:

```
# 源表信息
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/data
Found 52 items
-rw-r--r--   3 root hadoop        899 2022-09-27 16:45 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-074f2e7c-744b-4407-b367-3f9edfc938c7-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 16:39 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-0dfc0fa2-a6b0-43ed-a347-5dbe46caec49-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 13:57 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-13eada48-e851-4709-b491-b331eaa7f4e7-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 14:27 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-1e99e2dd-c56d-46fa-9058-e4c88c02f40a-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 14:10 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-2ac702ea-b403-439f-8818-20c8fd9f919c-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 17:47 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-2d34ed07-9429-4f3e-ac0f-09a496509014-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 16:46 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-36a4649d-5be0-4aa8-bed8-6c1f346c4fbf-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-37975cc3-0095-4fea-85eb-433e20220d81-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 14:19 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-381be99d-3509-47b6-8c75-4ae02610e10a-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 13:52 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-3d7db83a-9194-43a4-bc1e-b1ccfe4d475a-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 22:29 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-49350b3d-73cd-4e4f-920d-c6c12695b4d6-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 17:46 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-4b7a05be-7a20-41d1-b431-b50d2f3747b2-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 15:25 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-56217242-573d-44ca-af9e-c6f82d893997-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 15:24 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-6a9a9037-4a7c-493a-b1c5-021e1851a111-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 22:00 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-8a227177-799d-4418-a481-bab7b95253f0-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 17:50 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-8c7e92e7-20df-415b-aa54-f58da5b2d9dd-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 22:19 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-94404f26-1fb9-43e3-b187-9ee6c87eb590-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 22:16 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-9d51b384-8165-4054-a337-4cc8ab214583-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 22:32 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-ad0118bc-0d30-42a4-9dbd-32444571f028-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 14:13 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-b69d8fff-4a99-41df-877d-87d3c5a06b21-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 17:48 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-d19505d0-8eb1-4a79-88bb-d254b4c2f695-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 14:25 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-dc5d2176-20df-45e3-ab0c-35f291a03861-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 14:55 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-e1c07739-058a-466e-91fc-4ed86c154e26-00001.parquet
-rw-r--r--   3 root hadoop        899 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-e2020695-d924-425e-bd20-987b6c8e302e-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 15:27 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-faadcdf2-a79d-48e6-87e2-f2714c187941-00001.parquet
-rw-r--r--   3 root hadoop        871 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/data/00000-6-37710d74-2dcf-426c-812d-83da4b6a8d87-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 17:47 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-03be6140-4401-4388-800f-15c279c347b5-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 15:24 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-0cea68a8-fd69-4a76-bb0b-1a07d6032260-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 14:10 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-0f9f4040-2373-42a2-940c-9ea214130151-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 14:25 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-16890712-d27e-4927-b1cb-94a954c12604-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 16:45 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-2629b728-931d-4a5d-9c6f-c0e13a451c23-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 17:46 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-27ea065b-b1a1-4068-877d-2f8e128fba09-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 22:32 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-37b2d0fc-2659-4fd0-be57-ffcbcb5698eb-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 22:16 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-397f4535-07cf-4e48-a23c-707731262a4c-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 14:19 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-4080aacd-1319-414b-bcf0-6c66719454ab-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 16:46 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-43248a79-d70b-457c-a4c5-5a358e09506a-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 14:55 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-50b2b298-d290-41e7-ab53-1d384d3225dc-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 15:25 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-51b7e887-c93d-4d18-a02c-c61e91034d2f-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-5ff769f3-5349-4ced-8a94-78915b960eb0-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 13:57 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-6137e36b-64ae-4226-87fc-875980070bf0-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 17:48 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-6244ca14-5635-40c2-8544-550f6565ce5d-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 22:29 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-68287737-790b-4bf0-8f95-77841279bfe1-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 14:13 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-6e53e996-c1fd-44bf-92c7-3b4712119e65-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-796e100e-5663-47ad-b85d-8f82d2dc152a-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 16:39 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-ae5435e3-179a-4b4e-b0a4-6db50a77afe7-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 22:19 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-c0fc8c90-fb95-4cf3-bc3c-0b6de7fabf72-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 15:27 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-d4f917e7-3e82-4af9-bdac-29194a5508a6-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 17:50 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-db367e2d-ffae-4c8f-81d3-f6499191219f-00001.parquet
-rw-r--r--   3 root hadoop        920 2022-09-27 22:00 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-efb45de3-0eef-4990-9555-6a4bc10fce0f-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 14:27 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-f25ba1b6-d353-4b1c-880f-31b35497be55-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 13:52 /apps/hive/warehouse/iceberg_test_tbl/data/00001-1-f4f43424-d1d8-483f-a4a2-e467a6a3c83d-00001.parquet
-rw-r--r--   3 root hadoop        892 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/data/00001-7-7ff4951f-b00d-4faa-9110-5c4c68222c07-00001.parquet
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/data | wc -l
53

[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/metadata
Found 94 items
-rw-r--r--   3 root hadoop       1378 2022-09-27 13:52 /apps/hive/warehouse/iceberg_test_tbl/metadata/00000-843e73da-798d-4e2c-91d9-9057f9dd31a7.metadata.json
-rw-r--r--   3 root hadoop       2375 2022-09-27 13:52 /apps/hive/warehouse/iceberg_test_tbl/metadata/00001-2fe181fb-322e-4559-b479-104aac982ce3.metadata.json
-rw-r--r--   3 root hadoop       3410 2022-09-27 13:57 /apps/hive/warehouse/iceberg_test_tbl/metadata/00002-322c676e-d6b6-43e6-9742-b97a9751d886.metadata.json
-rw-r--r--   3 root hadoop       4445 2022-09-27 14:10 /apps/hive/warehouse/iceberg_test_tbl/metadata/00003-72109c79-c808-4937-9982-bce70774eeae.metadata.json
-rw-r--r--   3 root hadoop       5480 2022-09-27 14:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/00004-2977c81d-2c49-4d09-8145-7a47bc798569.metadata.json
-rw-r--r--   3 root hadoop       6517 2022-09-27 14:19 /apps/hive/warehouse/iceberg_test_tbl/metadata/00005-d8c3d0fc-3423-46ae-a065-d9e7b9b20d2c.metadata.json
-rw-r--r--   3 root hadoop       7555 2022-09-27 14:26 /apps/hive/warehouse/iceberg_test_tbl/metadata/00006-bb4cfed5-5c68-4857-8b68-b9b94997f507.metadata.json
-rw-r--r--   3 root hadoop       8593 2022-09-27 14:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/00007-cadbb861-fbb7-4e44-bddd-a67a4217c5f8.metadata.json
-rw-r--r--   3 root hadoop       9631 2022-09-27 14:55 /apps/hive/warehouse/iceberg_test_tbl/metadata/00008-f72a51c6-c140-4a02-933c-b607ecf32c12.metadata.json
-rw-r--r--   3 root hadoop      10669 2022-09-27 15:24 /apps/hive/warehouse/iceberg_test_tbl/metadata/00009-18fff4d7-8c35-4c8d-8013-76c0a81ee83c.metadata.json
-rw-r--r--   3 root hadoop      10941 2022-09-27 15:24 /apps/hive/warehouse/iceberg_test_tbl/metadata/00010-6aedd258-bbc7-4353-8593-3add08663859.metadata.json
-rw-r--r--   3 root hadoop      11972 2022-09-27 15:25 /apps/hive/warehouse/iceberg_test_tbl/metadata/00011-5c0f469f-14ca-4940-a0e6-7a4a47d39d21.metadata.json
-rw-r--r--   3 root hadoop      12245 2022-09-27 15:25 /apps/hive/warehouse/iceberg_test_tbl/metadata/00012-894da4d0-3926-4497-a023-ea021c551fc8.metadata.json
-rw-r--r--   3 root hadoop      13276 2022-09-27 15:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/00013-cb73fef2-93d4-4918-99a1-b95afa27968c.metadata.json
-rw-r--r--   3 root hadoop      13549 2022-09-27 15:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/00014-92d1cd57-b28b-4ae9-b4d9-3ae5acb73958.metadata.json
-rw-r--r--   3 root hadoop      14584 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/00015-fe80957d-65d4-4bf3-8d71-cf071a10e34e.metadata.json
-rw-r--r--   3 root hadoop      14856 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/00016-fc62b42a-5e44-495a-b34e-7451945f4ae1.metadata.json
-rw-r--r--   3 root hadoop      15891 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/00017-9baf32b3-6f35-4f02-baa3-f68a6f68aac6.metadata.json
-rw-r--r--   3 root hadoop      16163 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/00018-67112b64-b927-48bb-8bf2-8521e3e33c92.metadata.json
-rw-r--r--   3 root hadoop      16435 2022-09-27 16:22 /apps/hive/warehouse/iceberg_test_tbl/metadata/00019-033700e4-96b8-4386-9508-1430aee8e55c.metadata.json
-rw-r--r--   3 root hadoop      16707 2022-09-27 16:32 /apps/hive/warehouse/iceberg_test_tbl/metadata/00020-459bf81f-f1dc-49a7-9905-82cb6fe16294.metadata.json
-rw-r--r--   3 root hadoop      17745 2022-09-27 16:39 /apps/hive/warehouse/iceberg_test_tbl/metadata/00021-70ca5d36-afc0-484d-a87f-bb13a302a279.metadata.json
-rw-r--r--   3 root hadoop      18017 2022-09-27 16:39 /apps/hive/warehouse/iceberg_test_tbl/metadata/00022-0a67ae7d-387a-4d0b-8e60-db575a5376e1.metadata.json
-rw-r--r--   3 root hadoop      19055 2022-09-27 16:45 /apps/hive/warehouse/iceberg_test_tbl/metadata/00023-167ea11d-5195-4394-9274-1c799252bcc1.metadata.json
-rw-r--r--   3 root hadoop      19327 2022-09-27 16:45 /apps/hive/warehouse/iceberg_test_tbl/metadata/00024-a7f357ef-32eb-49e3-a6b0-0bbfc1b7c297.metadata.json
-rw-r--r--   3 root hadoop      20365 2022-09-27 16:46 /apps/hive/warehouse/iceberg_test_tbl/metadata/00025-498a1241-fea7-48b3-bc97-664376959f79.metadata.json
-rw-r--r--   3 root hadoop      20637 2022-09-27 16:46 /apps/hive/warehouse/iceberg_test_tbl/metadata/00026-9df3e713-a5d9-4c73-84e7-9dfa53bc464f.metadata.json
-rw-r--r--   3 root hadoop      21675 2022-09-27 17:46 /apps/hive/warehouse/iceberg_test_tbl/metadata/00027-c574a4b4-e737-462d-b1cb-76c81ee00c54.metadata.json
-rw-r--r--   3 root hadoop      22713 2022-09-27 17:47 /apps/hive/warehouse/iceberg_test_tbl/metadata/00028-5f39be00-8c7f-477c-9e76-c8bf392e5c40.metadata.json
-rw-r--r--   3 root hadoop      23751 2022-09-27 17:48 /apps/hive/warehouse/iceberg_test_tbl/metadata/00029-c40041a6-26d8-4a63-b586-7ab25b69cfe1.metadata.json
-rw-r--r--   3 root hadoop      24785 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/metadata/00030-df9738d8-2abb-410a-b870-3f8f26aadb01.metadata.json
-rw-r--r--   3 root hadoop      25058 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/metadata/00031-5de9f3df-6eff-4cd6-b346-f7c2702a0b45.metadata.json
-rw-r--r--   3 root hadoop      26092 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/metadata/00032-a3b4853d-ce6a-4325-a08d-8f9ff7d87998.metadata.json
-rw-r--r--   3 root hadoop      26365 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/metadata/00033-ea58a1f4-61f1-4ab5-b950-5eeb7d27e208.metadata.json
-rw-r--r--   3 root hadoop      27399 2022-09-27 22:00 /apps/hive/warehouse/iceberg_test_tbl/metadata/00034-e528e51b-cdaf-4508-8ac6-0e15d68e6240.metadata.json
-rw-r--r--   3 root hadoop      28437 2022-09-27 22:16 /apps/hive/warehouse/iceberg_test_tbl/metadata/00035-0d3a1586-8ce3-4b9c-b80c-d5c8a1bdb704.metadata.json
-rw-r--r--   3 root hadoop      28709 2022-09-27 22:16 /apps/hive/warehouse/iceberg_test_tbl/metadata/00036-6e28dc08-cb7a-47c1-8090-762dda27f685.metadata.json
-rw-r--r--   3 root hadoop      29747 2022-09-27 22:19 /apps/hive/warehouse/iceberg_test_tbl/metadata/00037-9ac60075-ec4d-4556-ac24-5f48f4486352.metadata.json
-rw-r--r--   3 root hadoop      30019 2022-09-27 22:19 /apps/hive/warehouse/iceberg_test_tbl/metadata/00038-31c842fb-06fb-4fdb-9eaa-a3b1ef4a1d31.metadata.json
-rw-r--r--   3 root hadoop      31057 2022-09-27 22:29 /apps/hive/warehouse/iceberg_test_tbl/metadata/00039-0be06d73-206f-4c99-a7fb-5d6733f03ecb.metadata.json
-rw-r--r--   3 root hadoop      32095 2022-09-27 22:32 /apps/hive/warehouse/iceberg_test_tbl/metadata/00040-e289f0cd-c0c2-477e-a7ee-605f09f1a237.metadata.json
-rw-r--r--   3 root hadoop      32367 2022-09-27 22:32 /apps/hive/warehouse/iceberg_test_tbl/metadata/00041-7edcc1a7-8ac1-4194-86a1-b322252a738f.metadata.json
-rw-r--r--   3 root hadoop       5930 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/020d2174-bbe3-433c-93af-990ca49fa03d-m0.avro
-rw-r--r--   3 root hadoop       5929 2022-09-27 17:48 /apps/hive/warehouse/iceberg_test_tbl/metadata/0572449f-18aa-4887-b880-8ba6bd00a1a0-m0.avro
-rw-r--r--   3 root hadoop       5927 2022-09-27 13:52 /apps/hive/warehouse/iceberg_test_tbl/metadata/17276271-0e96-4866-b7fa-639b1ecf8467-m0.avro
-rw-r--r--   3 root hadoop       5929 2022-09-27 13:57 /apps/hive/warehouse/iceberg_test_tbl/metadata/20b4f52a-1f52-42b5-91fe-26089c9cec4d-m0.avro
-rw-r--r--   3 root hadoop       5928 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/metadata/21844ffb-5d26-43e3-a2a8-f7e3ce1d45fd-m0.avro
-rw-r--r--   3 root hadoop       5925 2022-09-27 15:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/2bad1111-de64-4fab-a244-55198c3ff42d-m0.avro
-rw-r--r--   3 root hadoop       5928 2022-09-27 15:24 /apps/hive/warehouse/iceberg_test_tbl/metadata/308f15ba-8dd2-445d-9262-c54eade4b0ed-m0.avro
-rw-r--r--   3 root hadoop       5930 2022-09-27 22:00 /apps/hive/warehouse/iceberg_test_tbl/metadata/4473ce3e-5589-4859-8480-4e047dc27bb3-m0.avro
-rw-r--r--   3 root hadoop       5929 2022-09-27 16:39 /apps/hive/warehouse/iceberg_test_tbl/metadata/45d1df70-cad0-4b5f-a002-b78c150c886e-m0.avro
-rw-r--r--   3 root hadoop       5932 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/5bf1a920-bb3a-4b87-bb6e-20765c511fd0-m0.avro
-rw-r--r--   3 root hadoop       5933 2022-09-27 17:47 /apps/hive/warehouse/iceberg_test_tbl/metadata/68f89ee1-905a-43ad-94a0-6c4dc636490b-m0.avro
-rw-r--r--   3 root hadoop       5932 2022-09-27 22:29 /apps/hive/warehouse/iceberg_test_tbl/metadata/6ffc60c0-8a24-4f83-a5e5-4d0d011b118d-m0.avro
-rw-r--r--   3 root hadoop       5931 2022-09-27 17:46 /apps/hive/warehouse/iceberg_test_tbl/metadata/77f3f559-3731-4b2b-802e-58fefae37527-m0.avro
-rw-r--r--   3 root hadoop       5927 2022-09-27 14:10 /apps/hive/warehouse/iceberg_test_tbl/metadata/876c91cc-87bb-4fce-bfdd-1550d034c5f8-m0.avro
-rw-r--r--   3 root hadoop       5932 2022-09-27 16:45 /apps/hive/warehouse/iceberg_test_tbl/metadata/919b8ce9-60de-4506-bcb2-4fd2b7ba73ea-m0.avro
-rw-r--r--   3 root hadoop       5933 2022-09-27 22:32 /apps/hive/warehouse/iceberg_test_tbl/metadata/9a33ab95-ec21-49c6-bda7-b01b553d9db4-m0.avro
-rw-r--r--   3 root hadoop       5929 2022-09-27 14:19 /apps/hive/warehouse/iceberg_test_tbl/metadata/c0366829-33cb-42d2-8f3d-4990a2d19429-m0.avro
-rw-r--r--   3 root hadoop       5930 2022-09-27 14:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/c0f02a24-981c-49cb-9a0d-67cbdfe3ae2f-m0.avro
-rw-r--r--   3 root hadoop       5930 2022-09-27 14:55 /apps/hive/warehouse/iceberg_test_tbl/metadata/d08bd895-1a83-4842-a119-c54494c2b083-m0.avro
-rw-r--r--   3 root hadoop       5931 2022-09-27 17:50 /apps/hive/warehouse/iceberg_test_tbl/metadata/d190b39c-083d-4629-854d-fb42d773c006-m0.avro
-rw-r--r--   3 root hadoop       5929 2022-09-27 15:25 /apps/hive/warehouse/iceberg_test_tbl/metadata/dc4eaecf-2b59-4945-bd2d-46098d3b8ea1-m0.avro
-rw-r--r--   3 root hadoop       5932 2022-09-27 22:16 /apps/hive/warehouse/iceberg_test_tbl/metadata/e433307c-4353-4aa6-910b-d52b6b75cb5c-m0.avro
-rw-r--r--   3 root hadoop       5934 2022-09-27 16:46 /apps/hive/warehouse/iceberg_test_tbl/metadata/ecc507b8-8d75-475b-8480-c4b0fa363783-m0.avro
-rw-r--r--   3 root hadoop       5928 2022-09-27 14:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/f7a7e627-848e-407d-a551-f9dff5704821-m0.avro
-rw-r--r--   3 root hadoop       5929 2022-09-27 14:25 /apps/hive/warehouse/iceberg_test_tbl/metadata/fe639576-0a7b-43bf-9638-7f7bc0388883-m0.avro
-rw-r--r--   3 root hadoop       5931 2022-09-27 22:19 /apps/hive/warehouse/iceberg_test_tbl/metadata/fe63f8b9-a653-46b3-a163-f29bfc01e265-m0.avro
-rw-r--r--   3 root hadoop       4098 2022-09-27 22:29 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-1292882425929531684-1-6ffc60c0-8a24-4f83-a5e5-4d0d011b118d.avro
-rw-r--r--   3 root hadoop       3845 2022-09-27 13:57 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-1368006528896806597-1-20b4f52a-1f52-42b5-91fe-26089c9cec4d.avro
-rw-r--r--   3 root hadoop       3894 2022-09-27 14:10 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-1665322165591746063-1-876c91cc-87bb-4fce-bfdd-1550d034c5f8.avro
-rw-r--r--   3 root hadoop       4179 2022-09-27 17:48 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-1919321448752357960-1-0572449f-18aa-4887-b880-8ba6bd00a1a0.avro
-rw-r--r--   3 root hadoop       4214 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-200917155765321365-1-d190b39c-083d-4629-854d-fb42d773c006.avro
-rw-r--r--   3 root hadoop       4059 2022-09-27 14:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-3210846780360171248-1-c0f02a24-981c-49cb-9a0d-67cbdfe3ae2f.avro
-rw-r--r--   3 root hadoop       4096 2022-09-27 14:55 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-3268582405449064443-1-d08bd895-1a83-4842-a119-c54494c2b083.avro
-rw-r--r--   3 root hadoop       3891 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-3877921424867202910-1-020d2174-bbe3-433c-93af-990ca49fa03d.avro
-rw-r--r--   3 root hadoop       4139 2022-09-27 17:47 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-4510645946620756562-1-68f89ee1-905a-43ad-94a0-6c4dc636490b.avro
-rw-r--r--   3 root hadoop       4013 2022-09-27 14:25 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-4682874639393672542-1-fe639576-0a7b-43bf-9638-7f7bc0388883.avro
-rw-r--r--   3 root hadoop       4096 2022-09-27 17:51 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-533014079796221170-1-21844ffb-5d26-43e3-a2a8-f7e3ce1d45fd.avro
-rw-r--r--   3 root hadoop       3893 2022-09-27 15:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-606824877993706670-1-2bad1111-de64-4fab-a244-55198c3ff42d.avro
-rw-r--r--   3 root hadoop       3774 2022-09-27 13:52 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-609321932124834691-1-17276271-0e96-4866-b7fa-639b1ecf8467.avro
-rw-r--r--   3 root hadoop       4141 2022-09-27 22:32 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-6526971734319821437-1-9a33ab95-ec21-49c6-bda7-b01b553d9db4.avro
-rw-r--r--   3 root hadoop       4134 2022-09-27 15:24 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-6686836141026527903-1-308f15ba-8dd2-445d-9262-c54eade4b0ed.avro
-rw-r--r--   3 root hadoop       4098 2022-09-27 16:46 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-6811105265486824926-1-ecc507b8-8d75-475b-8480-c4b0fa363783.avro
-rw-r--r--   3 root hadoop       4098 2022-09-27 17:46 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-7279829749220627725-1-77f3f559-3731-4b2b-802e-58fefae37527.avro
-rw-r--r--   3 root hadoop       4137 2022-09-27 22:16 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-7352409308772439372-1-e433307c-4353-4aa6-910b-d52b6b75cb5c.avro
-rw-r--r--   3 root hadoop       4097 2022-09-27 16:39 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-8248285375762907729-1-45d1df70-cad0-4b5f-a002-b78c150c886e.avro
-rw-r--r--   3 root hadoop       4100 2022-09-27 16:45 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-8361479542460953047-1-919b8ce9-60de-4506-bcb2-4fd2b7ba73ea.avro
-rw-r--r--   3 root hadoop       3976 2022-09-27 14:19 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-8667987842706378050-1-c0366829-33cb-42d2-8f3d-4990a2d19429.avro
-rw-r--r--   3 root hadoop       3892 2022-09-27 15:25 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-874788846008064190-1-dc4eaecf-2b59-4945-bd2d-46098d3b8ea1.avro
-rw-r--r--   3 root hadoop       4097 2022-09-27 22:19 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-8880562242053368301-1-fe63f8b9-a653-46b3-a163-f29bfc01e265.avro
-rw-r--r--   3 root hadoop       3896 2022-09-27 16:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-8898936303218019847-1-5bf1a920-bb3a-4b87-bb6e-20765c511fd0.avro
-rw-r--r--   3 root hadoop       3936 2022-09-27 14:13 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-8961166628509057021-1-f7a7e627-848e-407d-a551-f9dff5704821.avro
-rw-r--r--   3 root hadoop       4095 2022-09-27 22:00 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-934385538870982327-1-4473ce3e-5589-4859-8480-4e047dc27bb3.avro
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/metadata | wc -l
95
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/metadata | grep snap |wc -l
26

```
执行如下：合并data files ，新生成一份文件
```scala
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

```

```shell script
# 新生成的data文件
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls -t /apps/hive/warehouse/iceberg_test_tbl/data 
Found 53 items
-rw-r--r--   3 root hadoop       1063 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/data/00000-0-36abff52-e845-408d-837a-81e9f97c0d7d-00001.parquet
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/data | wc -l
54

# 新生成的metadata文件
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls -t /apps/hive/warehouse/iceberg_test_tbl/metadata
Found 104 items
-rw-r--r--   3 root hadoop      33453 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/00042-a7ce2d78-48d2-481b-90e5-3e67ca4738b1.metadata.json
-rw-r--r--   3 root hadoop       3843 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/snap-5203335402409028148-1-fcf7aa79-d9de-46ab-b97c-915233757991.avro
-rw-r--r--   3 root hadoop       5881 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/fcf7aa79-d9de-46ab-b97c-915233757991-m7.avro
-rw-r--r--   3 root hadoop       5931 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/fcf7aa79-d9de-46ab-b97c-915233757991-m1.avro
-rw-r--r--   3 root hadoop       5932 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/fcf7aa79-d9de-46ab-b97c-915233757991-m2.avro
-rw-r--r--   3 root hadoop       5932 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/fcf7aa79-d9de-46ab-b97c-915233757991-m3.avro
-rw-r--r--   3 root hadoop       5930 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/fcf7aa79-d9de-46ab-b97c-915233757991-m0.avro
-rw-r--r--   3 root hadoop       5930 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/fcf7aa79-d9de-46ab-b97c-915233757991-m4.avro
-rw-r--r--   3 root hadoop       5931 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/fcf7aa79-d9de-46ab-b97c-915233757991-m5.avro
-rw-r--r--   3 root hadoop       5931 2022-09-27 23:27 /apps/hive/warehouse/iceberg_test_tbl/metadata/fcf7aa79-d9de-46ab-b97c-915233757991-m6.avro
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/metadata | wc -l
105
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/metadata | grep snap |wc -l
27
```
### 1.12 Iceberg 删除历史快照
目前可以通Java Api 删除历史快照，可以通过指定时间戳，当前时间戳之前的快照都会被删除，
注意：如果指定的时间比最后的快照时间还大，还是会保留最后一份快照数据，
可以通过查看元数据的json文件来查找指定的时间。
在删除快照的时候，数据data目录下过期的数据parquet文件也会删除，比如快照回滚后不需要的文件。

```scala
    table.expireSnapshots().expireOlderThan(1664292360000L).commit()
```

```shell script
#元数据文件
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/metadata | wc -l
54
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/metadata | grep snap |wc -l
1

#数据文件
[root@tbds-192-168-0-37 ~]# hdfs dfs -ls /apps/hive/warehouse/iceberg_test_tbl/data | wc -l
16
```


## 2. Flink 操作 Iceberg