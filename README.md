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

```
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
* months(ts):按照“年-月”月级别分区
* days(ts)或者date(ts):按照“年-月-日”天级别分区
* hours(ts)或者date_hour(ts):按照“年-月-日-时”小时级别分区

备注：Iceberg支持的时间分区目前和将来只支持UTC,UTC是国际时，UTC+8就是国际时加八小时，是东八区时间,也就是北京时间，所以我们看到上面分区时间与数据时间不一致。
除了以上常用的时间隐藏分区外，Iceberg还支持bucket(N,col)分区，这种分区方式可以按照某列的hash值与N取余决定数据去往的分区。truncate(L,col)，这种隐藏分区可以将字符串列截取L长度，相同的数据会被分到相同分区中。
**Partition Transforms **

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

## 2. Flink 操作 Iceberg
