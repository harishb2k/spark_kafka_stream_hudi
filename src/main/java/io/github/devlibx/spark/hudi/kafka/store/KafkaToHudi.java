package io.github.devlibx.spark.hudi.kafka.store;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.streaming.DataStreamReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "unchecked"})
public class KafkaToHudi {
    public static void main(String[] args) throws Exception {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        DataStreamReader dataStreamReader = sparkSession
                .readStream()
                .format("kafka")
                .option("rowsPerSecond", 100)
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic_payment_success_1")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", false);

        Dataset<Row> df = dataStreamReader
                .option("rowsPerSecond", 10)
                .load()
                .selectExpr(
                        "topic as kafka_topic",
                        "CAST(partition AS STRING) kafka_partition",
                        "CAST(timestamp as String) kafka_timestamp",
                        "CAST(offset AS STRING) kafka_offset",
                        "CAST(key AS STRING) kafka_key",
                        "CAST(value AS STRING) kafka_value",
                        "current_timestamp() current_time"
                ).selectExpr(
                        "kafka_topic",
                        "concat(kafka_partition,'-',kafka_offset) kafka_partition_offset",
                        "kafka_offset",
                        "kafka_timestamp",
                        "kafka_key",
                        "kafka_value",
                        "substr(current_time,1,10) partition_date"
                );

        // SparkRDDWriteClient writeClient = new SparkRDDWriteClient()

        //new HoodieSparkEngineContext(sparkSession.sparkContext())

        // HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
        //        .withPath(basePath)
        //        .forTable(tableName)
        //        .withSchema(schemaStr)
        //        .withProps(props) // pass raw k,v pairs from a property file.
        //        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withXXX(...).build())
        //        .withIndexConfig(HoodieIndexConfig.newBuilder().withXXX(...).build())

        Properties p = new Properties();
        p.put(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "kafka_timestamp");
        p.put(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "kafka_offset");
        p.put(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "kafka_key");
        p.put(HoodieWriteConfig.TABLE_NAME, "todo");
        HoodieWriteConfig hoodieCfg = HoodieWriteConfig.newBuilder()
                .withProperties(p)
                .withPath("/tmp/harish/data")
                .forTable("tt")
                .build();
        SparkRDDWriteClient writeClient = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), hoodieCfg);


        AtomicBoolean b = new AtomicBoolean(false);

        if (true) {
            df.toDF().write().format("org.apache.hudi")
                    .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "kafka_timestamp")
                    .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "kafka_offset")
                    .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "kafka_key")
                    .option(HoodieWriteConfig.TABLE_NAME, "todo")
                    .mode(SaveMode.Overwrite)
                    .save("file:///tmp/harish/data");
            return;
        }

        df.writeStream().queryName("demo").foreachBatch((v1, v2) -> {


            if (!b.get()) {
                b.set(true);
                v1.persist();
                v1.write().format("org.apache.hudi")
                        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "kafka_timestamp")
                        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "kafka_offset")
                        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "kafka_key")
                        .option(HoodieWriteConfig.TABLE_NAME, "todo")
                        .mode(SaveMode.Overwrite)
                        .save("file:///tmp/harish/data");
            }
            if (true) {
                return;
            }

            List<GenericRecord> records = new ArrayList<>();
            v1.foreach(row -> {
                Schema genericRecord = SchemaConverters.toAvroType(row.schema(), false, "record_name", "com.harish");
                GenericRecord rec = new GenericData.Record(genericRecord);
                for (String s : row.schema().fieldNames()) {
                    rec.put(s, row.getAs(s));
                }
                records.add(rec);
            });


            List<HoodieRecord> hoodieRecords = records.stream().map(genericRecord -> {
                HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), "partitionPath");
                return new HoodieRecord(key, new OverwriteWithLatestAvroPayload(Option.of(genericRecord)));
            }).collect(Collectors.toList());

            JavaRDD<HoodieRecord> items = jsc.parallelize(hoodieRecords, 1);
            // writeClient.insert(items, System.currentTimeMillis() + "");

//            JavaRDD<HoodieRecord<String>> rdd = null;

            // v1.map(v11 -> )

            // writeClient.


            /*System.out.println("--->>> start");
            v1.write().format("org.apache.hudi")
                    .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "kafka_timestamp")
                    .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "kafka_offset")
                    .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "kafka_key")
                    .option(HoodieWriteConfig.TABLE_NAME, "todo")
                    .mode(SaveMode.Overwrite)
                    .save("file:///Users/harishbohara/workspace/personal/new/kafka_hudi/data1");
            System.out.println("--->>> finish");
            v1.foreach(row -> {
                System.out.println("------->>>>>> " + v2 + " " + row.toString());
            });*/
            v1.unpersist();
        }).start().awaitTermination(5000000);
    }
}
