# bigdata-iceberg Spark��Flink����Iceberg

## 1. Spark ���� Iceberg
### 1.1 ǰ�� spark �� iceberg �汾��Ϣ
* spark 3.1.2
* iceberg 0.12.1
* hive 3.1.2
* hadoop 3.2.1

### 1.2 Spark����catalog
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

### 1.3 Spark��Iceberg DDL����
#### 1.3.1  CREATE TABLE ������
**create table ����Iceberg���������������Դ�����ͨ�������Դ�������������������в���һ������ʱ������Ҫ�������еķ����������򣬷��������ļ��رյĴ���**
```scala
    //������������loc�з����ֶ�
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
**����Iceberg����ʱ��������ͨ��һЩת�����ʽ��timestamp��������ת�������� ���������ط�����ת�����ʽ�����¼��֣�**
* years(ts):���������
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
ע�⣺���������ݣ�����Ҫ������������ֻҪ��ͬ�����д��һ��Ϳ���

* months(ts):���ա���-�¡��¼������
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

* days(ts)����date(ts):���ա���-��-�ա��켶�����

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
* hours(ts)����date_hour(ts):���ա���-��-��-ʱ��Сʱ�������
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
��ע��Iceberg֧�ֵ�ʱ�����Ŀǰ�ͽ���ֻ֧��UTC,UTC�ǹ���ʱ��UTC+8���ǹ���ʱ�Ӱ�Сʱ���Ƕ�����ʱ��,Ҳ���Ǳ���ʱ�䣬�������ǿ����������ʱ��������ʱ�䲻һ�¡����ǲ�ѯ��Ӱ���Զ�ת����
�������ϳ��õ�ʱ�����ط����⣬Iceberg��֧��bucket(N,col)���������ַ�����ʽ���԰���ĳ�е�hashֵ��Nȡ���������ȥ���ķ�����truncate(L,col)���������ط������Խ��ַ����н�ȡL���ȣ���ͬ�����ݻᱻ�ֵ���ͬ�����С�

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
Iceberg ֧�� ��**create table ... as select**���﷨�����ԴӲ�ѯ����д���һ�ű��������Ӧ������

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
#### 1.3.3 DROP TABLE ɾ��
ɾ����ʱ��Ŀ¼��Ȼ���ڣ�����dataĿ¼�µ������ļ���ɾ���ˡ�
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

#### 1.3.3 ALTER TABLE �޸ı�
Iceberg�� alter ������Spark3.x�汾��֧�֣�alterһ��������²�����
* ��ӡ�ɾ����
**����У�ALTER TABLE ... ADD COLUMN**
**ɾ���У�ALTER TABLE ... DROP COLUMN**
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
    //�����
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
    //ɾ����
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
## ԭʼ��
+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|   rison| 18|
|  2|zhagnsan| 20|
+---+--------+---+
## ��� gender/loc ��
+---+--------+---+------+----+
| id|    name|age|gender| loc|
+---+--------+---+------+----+
|  1|   rison| 18|  null|null|
|  2|zhagnsan| 20|  null|null|
+---+--------+---+------+----+
## ɾ��age��
+---+--------+------+----+
| id|    name|gender| loc|
+---+--------+------+----+
|  1|   rison|  null|null|
|  2|zhagnsan|  null|null|
+---+--------+------+----+

```
* ��������
**�������У�ALTER TABLE ... RENAME COLUMN**

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
## ԭʼ��
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
## �޸�idΪid_rename
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
#### 1.3.4 ALTER TABLE �޸ķ���
alter�޸ķ�����������ӷ�����ɾ�����������ַ���������spark3.x֮��֧�֣�
ʹ��֮ǰ����Ҫ���spark.sql.extensions���ԣ���ֵΪ��org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
����ӷ���ʱ��֧�ַ���ת�����﷨���£�
* ��ӷ�����ALTER TABLE...ADD PARTITION FIELD
```scala
   //����������

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
    //���locΪ����
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
    //���years(ts)Ϊ����
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
��ӷ����ֶ���Ԫ���ݲ���������ı����еı����ݣ��µ����ݽ�ʹ���·���д�����ݣ��������ݽ�����������ԭ�еķ��������С�

* ɾ��������ALTER TABLE...DROP PARTITION FIELD

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
���Ƿ��֣�ɾ�����loc������years(ts)����֮��,Ŀ¼���**loc=null/ts_year=null**������������ݽ������ڸ�·���¡�

### 1.4 DataFrame API����Iceberg�е�����
Spark ����Iceberg ��������ͨ��SQL�ķ�ʽ��ѯIceberg���ݣ�������ʹ��dataFrame�ķ�ʽ���ص�Iceberg���У�
����ͨ��spark.table(����)����spark.read.format(iceberg).load(iceberg data path)�����ض�Ӧ�ı����ݣ�
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
    // 1.1 sql�ķ�ʽ��ȡiceberg������
    spark.sql("select * from hive_catalog.default.iceberg_test_tbl").show()

    // 1.2 dataframe�ķ�ʽ��ȡiceberg������
    spark.table("hive_catalog.default.iceberg_test_tbl").show()
    spark.read.format("iceberg")
      .load("hdfs://hdfsCluster/apps/hive/warehouse/iceberg_test_tbl").show()

```










## 2. Flink ���� Iceberg