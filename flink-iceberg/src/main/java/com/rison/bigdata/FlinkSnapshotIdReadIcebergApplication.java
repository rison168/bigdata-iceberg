package com.rison.bigdata;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: main
 * @USER: Rison
 * @DATE: 2022/10/2 17:33
 * @PROJECT_NAME: bigdata-iceberg
 **/
public class FlinkSnapshotIdReadIcebergApplication {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 设置 checkpoint, flink向Iceberg写入数据，只有checkpoint触发后，才会commit数据
        env.enableCheckpointing(5_000);

        //TODO 创建iceberg 库表
        TableIdentifier identifier = TableIdentifier.of(Namespace.of("iceberg_db"), "stream_iceberg_tbl");
        final CatalogLoader catalogLoader = catalogLoader("hive_catalog");
        if (!catalogLoader.loadCatalog().tableExists(identifier)) {
            final Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "name", Types.StringType.get()),
                    Types.NestedField.required(3, "age", Types.IntegerType.get()),
                    Types.NestedField.required(4, "loc", Types.StringType.get())
            );
            //不设置分区
//            final PartitionSpec spec = PartitionSpec.unpartitioned();
            //设置分区
            final PartitionSpec spec = PartitionSpec.builderFor(schema).identity("loc").build();
            //指定存储格式
            final ImmutableMap<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());
            catalogLoader.loadCatalog().createTable(identifier, schema, spec, props);
        }
        final TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        //TODO 读取iceberg
        final DataStream<RowData> data = FlinkSource.forRowData()
                .env(env)
                .startSnapshotId(4987625117265033885L)
                //默认为false,批读，可以设置为true为流读
                .streaming(false)
                .tableLoader(tableLoader)
                .build();

        data.print();
        env.execute("FlinkSnapshotIdReadIcebergApplication");
    }

    public static Configuration hadoopConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hdfsCluster");
        configuration.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/hdfs-site.xml"));
        configuration.addResource(new Path("/usr/hdp/current/hadoop-client/etc/hadoop/core-site.xml"));
        configuration.addResource(new Path("/usr/hdp/current/hive-client/conf/hive-site.xml"));
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromSubject(null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return configuration;
    }

    public static CatalogLoader catalogLoader(String catalog) {
        HashMap<String, String> map = new HashMap<>();
        map.put("type", "iceberg");
        map.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, FlinkCatalogFactory.ICEBERG_CATALOG_TYPE_HIVE);
        map.put(CatalogProperties.WAREHOUSE_LOCATION, "hdfs://apps/hive/warehouse/");
        map.put(CatalogProperties.URI, "thrift://tbds-192-168-0-18:9083,thrift://tbds-192-168-0-29:9083");
        map.put(CatalogProperties.CLIENT_POOL_SIZE, "5");
        return CatalogLoader.hive(catalog, hadoopConfiguration(), map);
    }
}
/*
/usr/hdp/2.2.0.0-2041/flink/bin/flink run \
-t yarn-per-job \
-p 1 \
-c com.rison.bigdata.FlinkSnapshotIdReadIcebergApplication \
/root/flink-dir/flink-iceberg.jar
 */