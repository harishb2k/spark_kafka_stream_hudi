package io.github.devlibx.spark.hudi.kafka.store;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class KafkaToCsv {
    public static void main(String[] args) throws StreamingQueryException {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        DataStreamReader dataStreamReader = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic_payment_success")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", false);

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
                    v1.write().format("com.databricks.spark.csv")
                            .save("./data/" + v2.toString() + "_mydata.csv");
                })
                .start().awaitTermination();
    }
}
