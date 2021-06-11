package io.github.devlibx.spark.hudi.kafka.store;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws InterruptedException, StreamingQueryException {

        // https://programming.vip/docs/actual-write-to-hudi-using-spark-streaming.html
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
      /*  sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic_payment_success")
                // .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", 100000)
                .option("failOnDataLoss", false)
                .load()
                .write()
                .format("com.databricks.spark.csv")
                .save("mydata.csv");*/


        // Define kafka flow
        DataStreamReader dataStreamReader = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic_payment_success")
                .option("startingOffsets", "latest")
                // .option("maxOffsetsPerTrigger", 100000)
                .option("failOnDataLoss", false);

        // Loading stream data, because it is only for testing purposes, reading kafka messages directly without any other processing, is that spark structured streams automatically generate kafka metadata for each set of messages, such as the subject of the message, partition, offset, and so on.
        Dataset<Row> df = dataStreamReader.load()
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

        df.writeStream().format("com.databricks.spark.csv")
                .foreachBatch((v1, v2) -> {
                    /*v1.write().format("org.apache.hudi")
                            .option(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "kafka_partition_offset")
                            .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition_date")
                            .option(HoodieWriteConfig.TABLE_NAME, "copy_on_write_table")
                            .option(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, true)
                            .mode(SaveMode.Append)
                            .save("/tmp/sparkHudi/COPY_ON_WRITE");*/
                    v1.write().format("com.databricks.spark.csv")
                            .save("./data/" + v2.toString() + "_mydata.csv");
                })
                .start().awaitTermination();

        if (true) {
            return;
        }

        //
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("topic_payment_success");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );
        JavaPairDStream<String, String> o = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        o.foreachRDD((v1, v2) -> {
            v1.foreach(stringStringTuple2 -> {
                System.out.println(stringStringTuple2._1 + "-----" + stringStringTuple2._2);
            });
            System.out.println("------> " + v2.toString());
        });


        jssc.start();
        jssc.awaitTermination();

    }
}
