package com.rison.bigdata;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: FlinkSqlWriteIcebergApplication
 * @USER: Rison
 * @DATE: 2022/10/6 16:01
 * @PROJECT_NAME: bigdata-iceberg
 **/
public class FlinkSqlWriteIcebergApplicationV2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env, settings);
        //TODO 设置 checkpoint, flink向Iceberg写入数据，只有checkpoint触发后，才会commit数据
        env.enableCheckpointing(1_000);
        final Configuration configuration = tblEnv.getConfig().getConfiguration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);
        configuration.setString("execution.type", "streaming");
        configuration.setString("pipeline.name", FlinkSqlReadIcebergApplication.class.getName());

        String mysqlCDCSQL = "CREATE TABLE IF NOT EXISTS mysql_student (\n" +
                "     id INT,\n" +
                "     name STRING,\n" +
                "     description STRING,\n" +
                "     PRIMARY KEY (id) NOT ENFORCED\n" +
                "   ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'tbds-192-168-0-37',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = 'metadata@Tbds.com',\n" +
                "     'scan.startup.mode'='latest-offset',\n" +
                "     'database-name' = 'rison_db',\n" +
                "     'table-name' = 'student'\n" +
                "   )";

        tblEnv.executeSql(mysqlCDCSQL);

        //如果表不存在会自动创建库表,注意：upsert 操作需要 v2 表支持，同时需要设置主键，否则update会产生重复数据。
        tblEnv.executeSql("create database if not exists iceberg_db");
        String icebergCDCSQL = " CREATE TABLE if not exists iceberg_student (\n" +
                " id int,\n" +
                " name STRING,\n" +
                " description STRING,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                " ) WITH (\n" +
                " 'format-version'='2', \n" +
                " 'connector'='iceberg', \n" +
                " 'catalog-name' = 'iceberg_catalog', \n" +
                " 'type' = 'iceberg', \n" +
                " 'catalog-type' = 'hive', \n" +
                " 'uri' = 'thrift://tbds-192-168-0-18:9083,thrift://tbds-192-168-0-29:9083', \n" +
                " 'clients' = '5', \n" +
                " 'warehouse'='hdfs:///apps/hive/warehouse', \n" +
                " 'catalog-table' = 'iceberg_student', \n" +
                " 'catalog-database' = 'iceberg_db'\n" +
                " )";
        tblEnv.executeSql(icebergCDCSQL);
        tblEnv.executeSql("insert into iceberg_student select * from mysql_student");

    }
}
/*
/usr/hdp/2.2.0.0-2041/flink/bin/flink run \
-t yarn-per-job \
-p 1 \
-c com.rison.bigdata.FlinkSqlWriteIcebergApplicationV2 \
/root/flink-dir/original-flink-iceberg.jar
 */