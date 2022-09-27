package com.rison.bigdata

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.HiveCatalog
import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: SparkRollBackSnapshotApplication
 * @USER: Rison
 * @DATE: 2022/9/27 17:58
 * @PROJECT_NAME: bigdata-iceberg
 * */

object SparkRollBackSnapshotApplication {
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
      //修改分区属性需要
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()
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
    spark.close()

  }
}
/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.bigdata.SparkRollBackSnapshotApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/iceberg-spark.jar
 */