package io.github.devlibx.spark.hudi.kafka.store;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;

public class Main {
    public static void main(String[] args) throws Exception {
         // HoodieDeltaStreamer.main(new String[]{"--help"});
         if (true) {
             // return;
         }


        HoodieDeltaStreamer.main(new String[]{
                "--table-type", "COPY_ON_WRITE",
                "--source-class", "org.apache.hudi.utilities.sources.JsonKafkaSource",
//                "--class", "org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer",
                "--source-ordering-field", "ts",
                "--target-base-path", "/Users/harishbohara/workspace/personal/new/kafka_hudi/data1",
                "--target-table", "stock_ticks_mor",
                "--props", "/Users/harishbohara/workspace/personal/new/kafka_hudi/kafka-source.properties",
                // "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider",
        });
    }

}

// spark-submit --packages org.apache.hudi:hudi-utilities-bundle_2.11:0.5.1-incubating,org.apache.spark:spark-avro_2.11:2.4.4 \
// --master yarn \
// --deploy-mode cluster \
// --num-executors 10 \
// --executor-memory 3g \
// --driver-memory 6g \
// --conf spark.driver.extraJavaOptions="-XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/varadarb_ds_driver.hprof" \
// --conf spark.executor.extraJavaOptions="-XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/varadarb_ds_executor.hprof" \
// --queue hadoop-platform-queue \
// --conf spark.scheduler.mode=FAIR \
// --conf spark.yarn.executor.memoryOverhead=1072 \
// --conf spark.yarn.driver.memoryOverhead=2048 \
// --conf spark.task.cpus=1 \
// --conf spark.executor.cores=1 \
// --conf spark.task.maxFailures=10 \
// --conf spark.memory.fraction=0.4 \
// --conf spark.rdd.compress=true \
// --conf spark.kryoserializer.buffer.max=200m \
// --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
// --conf spark.memory.storageFraction=0.1 \
// --conf spark.shuffle.service.enabled=true \
// --conf spark.sql.hive.convertMetastoreParquet=false \
// --conf spark.ui.port=5555 \
// --conf spark.driver.maxResultSize=3g \
// --conf spark.executor.heartbeatInterval=120s \
// --conf spark.network.timeout=600s \
// --conf spark.eventLog.overwrite=true \
// --conf spark.eventLog.enabled=true \
// --conf spark.eventLog.dir=hdfs:///user/spark/applicationHistory \
// --conf spark.yarn.max.executor.failures=10 \
// --conf spark.sql.catalogImplementation=hive \
// --conf spark.sql.shuffle.partitions=100 \
// --driver-class-path $HADOOP_CONF_DIR \
// --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
// --table-type MERGE_ON_READ \
// --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
// --source-ordering-field ts  \
// --target-base-path /user/hive/warehouse/stock_ticks_mor \
// --target-table stock_ticks_mor \
// --props /var/demo/config/kafka-source.properties \
// --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider