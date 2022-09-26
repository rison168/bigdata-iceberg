"# bigdata-iceberg Spark和Flink操作Iceberg" 

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



## 2. Flink 操作 Iceberg
