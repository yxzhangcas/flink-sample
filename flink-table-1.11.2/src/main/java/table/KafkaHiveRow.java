package table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class KafkaHiveRow {
    final static String kafkaConnection = "'connector'='kafka', 'topic'='test_kafka', " +
            "'properties.bootstrap.servers'='10.0.24.124:9092', " +
            "'properties.group.id'='testGroup', " +
            "'scan.startup.mode'='latest-offset', " +
            "'format'='csv', 'csv.field-delimiter'='|', " +
            "'csv.line-delimiter'=U&'\\000A', 'csv.ignore-parse-errors'='true'";
    final static String kafkaDDLSingleColumn = "CREATE TABLE kTable ( column1 STRING ) WITH (" + kafkaConnection + " )";
    final static String kafkaDDLTwoColumns = "CREATE TABLE kTable ( column1 STRING, column2 STRING ) WITH (" + kafkaConnection + " )";

    final static String hiveFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'";
    final static String hiveDDLSingleColumnNoPartition = "CREATE TABLE hTable ( column1 STRING ) " + hiveFormat;
    final static String hiveDDLTwoColumnsNoPartition = "CREATE TABLE hTable ( column1 STRING, column2 STRING ) " + hiveFormat;

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
        //tableEnv.executeSql(kafkaDDLSingleColumn);
        tableEnv.executeSql(kafkaDDLTwoColumns);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("DROP TABLE IF EXISTS hTable");
        //tableEnv.executeSql(hiveDDLSingleColumnNoPartition);
        tableEnv.executeSql(hiveDDLTwoColumnsNoPartition);

        tableEnv.executeSql("INSERT INTO hTable SELECT * FROM kTable");
    }
}

/*
kafka-console-producer.sh --broker-list localhost:9092 --topic test_kafka
select * from htable;

1.SingleColumn
    空行 -> 无输出
    a -> a
    a| -> a
    a|b -> 无输出

2.TwoColumns
    空行     ->   无输出
    a       ->  a       NULL
    a|      ->  a       空字符串
    a|b     ->  a       b
    a|b|    ->  a       b
    a|b|c   ->  无输出

 */