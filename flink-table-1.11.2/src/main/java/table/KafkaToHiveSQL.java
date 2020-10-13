package table;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

public class KafkaToHiveSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));

        String catalogName = "my_catalog";
        HiveCatalog catalog = new HiveCatalog(catalogName, "default",
                "."   //Local IDE
                //"/etc/hive/conf"    //cluster
        );
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        tableEnv.executeSql("DROP TABLE IF EXISTS kafka_table");
        tableEnv.executeSql("CREATE TABLE kafka_table ( " +
                "user_id STRING, " +
                "order_amount DOUBLE, " +
                "log_ts TIMESTAMP(3), " +
                "WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'my_topic'," +
                "'properties.bootstrap.servers' = '10.0.24.124:9092,10.0.24.127:9092,10.0.24.128:9092'," +
                "'properties.group.id' = 'flink_hive_test'," +
                "'scan.startup.mode'='latest-offset', " +
                "'format' = 'json'," +
                "'json.fail-on-missing-field' = 'false'," +
                "'json.ignore-parse-errors' = 'true'" +
                ")");


        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("DROP TABLE IF EXISTS hive_table");

        tableEnv.executeSql("CREATE TABLE hive_table (" +
                "user_id STRING," +
                "order_amount DOUBLE" +
                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (" +
                "'partition.time-extractor.timestamp-pattern' = '$dt $hr:00:00'," +
                "'sink.partition-commit.trigger' = 'partition-time'," +
                "'sink.partition-commit.delay' = '1 min'," +
                "'sink.partition-commit.policy.kind' = 'metastore,success-file'" +
                ")");

        tableEnv.executeSql("INSERT INTO TABLE hive_table " +
                "SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') " +
                "FROM kafka_table");

    }
}

/*
向Kafka生产的测试数据：
    kafka-console-producer.sh --broker-list localhost:9092 --topic my_topic
        {"user_id":"a1111","order_amount":11.0,"log_ts":"2020-06-29 12:12:12"}
        {"user_id":"a1111","order_amount":11.0,"log_ts":"2020-06-29 12:15:00"}
        {"user_id":"a1111","order_amount":11.0,"log_ts":"2020-06-29 12:20:00"}
        {"user_id":"a1111","order_amount":11.0,"log_ts":"2020-06-29 12:30:00"}
        {"user_id":"a1111","order_amount":13.0,"log_ts":"2020-06-29 12:32:00"}
从Hive表中读取的结果：
    select * from hive_table;
        a1111	11.0	2020-06-29	12
        a1111	11.0	2020-06-29	12
        a1111	11.0	2020-06-29	12
        a1111	11.0	2020-06-29	12
        a1111	13.0	2020-06-29	12
 */

/*
需要向flink安装目录的lib中上传的jar包：
flink-shaded-hadoop-2-uber-2.6.5-10.0.jar           #java.lang.ClassNotFoundException: org.apache.hadoop.mapred.JobConf
flink-sql-connector-hive-1.2.2_2.11-1.11.2.jar      #java.lang.ClassNotFoundException: org.apache.flink.table.catalog.hive.HiveCatalog
flink-sql-connector-kafka_2.12-1.11.2.jar           #Could not find any factory for identifier 'kafka' that implements 'org.apache.flink.table.factories.DynamicTableSourceFactory' in the classpath.
 */