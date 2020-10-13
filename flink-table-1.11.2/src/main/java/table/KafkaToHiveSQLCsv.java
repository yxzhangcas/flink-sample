package table;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import java.time.Duration;

public class KafkaToHiveSQLCsv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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

        tableEnv.executeSql("DROP TABLE IF EXISTS KafkaSQLCsv");
        tableEnv.executeSql("CREATE TABLE KafkaSQLCsv ( " +
                "user_id STRING, " +
                "order_amount DOUBLE, " +
                //"log_ts TIMESTAMP(3)" +
                "log_ts TIMESTAMP(3), " +
                //生成watermark是必须的，否则hive表的元数据无法进行更新。（类似于事务无法提交）
                "WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'KafkaToHiveSQLCsv'," +
                "'properties.bootstrap.servers' = '10.0.24.124:9092,10.0.24.127:9092,10.0.24.128:9092'," +
                "'properties.group.id' = 'KafkaToHiveSQLCsv'," +
                //调试期间始终从末尾开始消费，只处理后续输入的测试数据
                "'scan.startup.mode'='latest-offset', " +
                "'format' = 'csv'," +
                "'csv.field-delimiter'='|'" +
                ")");


        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("DROP TABLE IF EXISTS HiveSQLCsv");
        tableEnv.executeSql("CREATE TABLE HiveSQLCsv (" +
                "user_id STRING," +
                "order_amount DOUBLE" +
                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (" +
                "'partition.time-extractor.timestamp-pattern' = '$dt $hr:00:00'," +
                "'sink.partition-commit.trigger' = 'partition-time'," +
                "'sink.partition-commit.delay' = '1 min'," +
                "'sink.partition-commit.policy.kind' = 'metastore,success-file'" +
                ")");

        tableEnv.executeSql("INSERT INTO TABLE HiveSQLCsv " +
                //可以通过人工指定字段进行输出
                //"SELECT user_id, order_amount, '2020-06-06', '06' " +
                "SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') " +
                "FROM KafkaSQLCsv");

        //tableEnv.toAppendStream(tableEnv.sqlQuery("select * from KafkaSQLCsv"), Row.class).print();
        //env.execute();
    }
}

/*
向Kafka生产的测试数据：
    kafka-console-producer.sh --broker-list localhost:9092 --topic KafkaToHiveSQLCsv
        a1111|11.0|2020-06-29 12:12:12
从Hive表中读取的结果：
    select * from hivesqlcsv;
        a1111	11.0	2020-06-29	12
 */

/*
需要向flink安装目录的lib中上传的jar包：
flink-shaded-hadoop-2-uber-2.6.5-10.0.jar           #java.lang.ClassNotFoundException: org.apache.hadoop.mapred.JobConf
flink-sql-connector-hive-1.2.2_2.11-1.11.2.jar      #java.lang.ClassNotFoundException: org.apache.flink.table.catalog.hive.HiveCatalog
flink-sql-connector-kafka_2.12-1.11.2.jar           #Could not find any factory for identifier 'kafka' that implements 'org.apache.flink.table.factories.DynamicTableSourceFactory' in the classpath.
 */