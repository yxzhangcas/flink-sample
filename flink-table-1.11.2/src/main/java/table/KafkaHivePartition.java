package table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class KafkaHivePartition {
    final static String kafkaConnection = "'connector'='kafka', 'topic'='test_kafka', " +
            "'properties.bootstrap.servers'='10.0.24.124:9092', " +
            "'properties.group.id'='testGroup', " +
            "'scan.startup.mode'='latest-offset', " +
            "'format'='csv', 'csv.field-delimiter'='|', " +
            "'csv.line-delimiter'=U&'\\000A', 'csv.ignore-parse-errors'='true'";
    final static String kafkaDDLTwoColumns = "CREATE TABLE kTable ( column1 STRING, column2 STRING ) WITH (" + kafkaConnection + " )";
    final static String kafkaDDLThreeColumns = "CREATE TABLE kTable ( column1 STRING, column2 STRING, column3 STRING ) WITH (" + kafkaConnection + " )";

    final static String hiveFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS PARQUET TBLPROPERTIES ( " +
            "'sink.partition-commit.trigger'='process-time', " +
            "'sink.partition-commit.delay'='0 s', " +
            //下面这行是分区提交的关键，分区变化需要元数据对应更新，所以需要提交到metastore，success-file是一个_SUCCESS文件，应该跟PARQUET目录有关
            "'sink.partition-commit.policy.kind' = 'metastore,success-file' )";
    final static String hiveDDLSingleColumnOnePartitionLayer = "CREATE TABLE hTable ( column1 STRING ) PARTITIONED BY ( column2 STRING ) " + hiveFormat;
    final static String hiveDDLSingleColumnTwoPartitionLayers = "CREATE TABLE hTable ( column1 STRING ) PARTITIONED BY ( column2 STRING, column3 STRING ) " + hiveFormat;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //hive的table是基于filesystem.streaming-sink的，要求必须开启Checkpoint机制，否则无法实现2PC导致不能提交
        //还可以使用相关的配置：row/bulk的不同文件滚动策略，不过分区还是要使用hive的DDL进行实现
        env.enableCheckpointing(1000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String catalogName = "my_catalog";
        HiveCatalog catalog = new HiveCatalog(catalogName, "default", ".");
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        tableEnv.executeSql("DROP TABLE IF EXISTS kTable");
        //tableEnv.executeSql(kafkaDDLTwoColumns);
        tableEnv.executeSql(kafkaDDLThreeColumns);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("DROP TABLE IF EXISTS hTable");
        //tableEnv.executeSql(hiveDDLSingleColumnOnePartitionLayer);
        tableEnv.executeSql(hiveDDLSingleColumnTwoPartitionLayers);

        tableEnv.executeSql("INSERT INTO hTable SELECT * FROM kTable");
    }
}

/*
kafka-console-producer.sh --broker-list localhost:9092 --topic test_kafka
select * from htable;

1.hiveDDLSingleColumnOnePartitionLayer
>               -> 无输出
>a              -> a	__HIVE_DEFAULT_PARTITION__
>a|             -> a	__HIVE_DEFAULT_PARTITION__
>a|b            -> a	b
>a|b|           -> a	b
>a|b|c          -> 无输出

2.hiveDDLSingleColumnTwoPartitionLayers
>               ->  无输出
>a              ->  a	__HIVE_DEFAULT_PARTITION__	__HIVE_DEFAULT_PARTITION__
>a|             ->  a	__HIVE_DEFAULT_PARTITION__	__HIVE_DEFAULT_PARTITION__
>a|b            ->  a	b	__HIVE_DEFAULT_PARTITION__
>a|b|           ->  a	b	__HIVE_DEFAULT_PARTITION__
>a|b|c          ->  a	b	c
>a|b|c|         ->  a	b	c
>a|b|c|d

 */