package com.rison.bigdata;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * @PACKAGE_NAME: com.rison.bigdata
 * @NAME: FlinkStreamApiWriteIcebergApplication
 * @USER: Rison
 * @DATE: 2022/10/2 15:07
 * @PROJECT_NAME: bigdata-iceberg
 **/
public class FlinkStreamApiWriteIcebergApplication {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 设置 checkpoint, flink向Iceberg写入数据，只有checkpoint触发后，才会commit数据
        env.enableCheckpointing(5_000);
        //TODO 读取 kafka中的数据
        final String kafkaServers = "tbds-192-168-0-29:6669,tbds-192-168-0-30:6669,tbds-192-168-0-31:6669";
        final String topic = "flink_iceberg_topic";
        final Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafka\" password=\"kafka@Tbds.com\";");
        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics(topic)
                .setGroupId("test-group-id")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();
        final DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "flink_iceberg_topic");
        dataStreamSource.print();
        //TODO 对数据进行处理，包装成RowData 对象， 方便写到iceberg
        final SingleOutputStreamOperator<RowData> rowDataStream = dataStreamSource.map(
                new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String data) throws Exception {
                        final String[] dataArr = data.split(",");
                        GenericRowData row = new GenericRowData(data.length());
                        row.setField(0, Integer.valueOf(dataArr[0]));
                        row.setField(1, StringData.fromString(dataArr[1]));
                        row.setField(2, Integer.valueOf(dataArr[2]));
                        row.setField(3, StringData.fromString(dataArr[3]));
                        return row;
                    }
                }
        );

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
            catalogLoader.loadCatalog().newCreateTableTransaction(identifier, schema, spec, props);
        }
        final TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);

        //TODO 通过DataStream APi 往iceberg 写数据
        FlinkSink.forRowData(rowDataStream)
                .tableLoader(tableLoader)
                //默认false，设置true为覆盖
                .overwrite(false)
                .build();
        env.execute("FlinkStreamApiWriteIcebergApplication");
    }

    public static Configuration hadoopConfiguration() {
        final Configuration configuration = new Configuration();
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
        final HashMap<String, String> map = new HashMap<>();
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
-c com.rison.bigdata.FlinkStreamApiWriteIcebergApplication \
/root/flink-dir/flink-iceberg.jar
 */