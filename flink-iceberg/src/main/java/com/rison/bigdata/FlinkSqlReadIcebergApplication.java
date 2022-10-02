package com.rison.bigdata;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: FlinkSqlReadIceberg
 * @USER: Rison
 * @DATE: 2022/10/2 17:53
 * @PROJECT_NAME: bigdata-iceberg
 **/
public class FlinkSqlReadIcebergApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env, settings);

        //TODO 设置 checkpoint, flink向Iceberg写入数据，只有checkpoint触发后，才会commit数据
        env.enableCheckpointing(1_000);
        final Configuration configuration = tblEnv.getConfig().getConfiguration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);
        configuration.setString("execution.type", "streaming");
        configuration.setString("pipeline.name", FlinkSqlReadIcebergApplication.class.getName());
        //创建iceberg_catalog
        String catalogSQL = "CREATE CATALOG iceberg_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://tbds-192-168-0-18:9083,thrift://tbds-192-168-0-29:9083',\n" +
                "  'clients'='5',\n" +
                "  'property-version'='1',\n" +
                "  'warehouse'='hdfs:///apps/hive/warehouse'\n" +
                ")";

        tblEnv.executeSql(catalogSQL);
        //批读
//       tblEnv.executeSql("SELECT * FROM iceberg_catalog.iceberg_db.stream_iceberg_tbl").print();
        //实时增量读
        tblEnv.executeSql("SELECT * FROM iceberg_catalog.iceberg_db.stream_iceberg_tbl /* + OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ").print();

    }
}
/*
/usr/hdp/2.2.0.0-2041/flink/bin/flink run \
-t yarn-per-job \
-p 1 \
-c com.rison.bigdata.FlinkSqlReadIcebergApplication \
/root/flink-dir/flink-iceberg.jar
 */