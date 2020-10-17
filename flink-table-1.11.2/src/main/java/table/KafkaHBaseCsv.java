package table;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class KafkaHBaseCsv {
    final static String kafkaConnection = "'connector'='kafka', 'topic'='test_kafka', " +
            "'properties.bootstrap.servers'='10.0.24.124:9092', " +
            "'properties.group.id'='testGroup', " +
            "'scan.startup.mode'='latest-offset', " +
            "'format'='csv', 'csv.field-delimiter'='|', " +
            "'csv.line-delimiter'=U&'\\000A', 'csv.ignore-parse-errors'='true'";
    final static String kafkaDDLSingleColumn = "CREATE TABLE kTable ( rowkey STRING ) WITH (" + kafkaConnection + " )";
    final static String kafkaDDLTwoColumns = "CREATE TABLE kTable ( rowkey STRING, column1 STRING ) WITH (" + kafkaConnection + " )";
    final static String kafkaDDLThreeColumns = "CREATE TABLE kTable ( column1 STRING, column2 STRING, column3 STRING ) WITH (" + kafkaConnection + " )";

    final static String hbaseConnection = "'connector'='hbase-1.4', 'table-name'='test_hbase', " +
            "'zookeeper.quorum'='10.0.24.124', " +
            "'sink.buffer-flush.max-size'='0', " +
            "'sink.buffer-flush.max-rows'='0'";
    final static String hbaseDDLSingleColumn = "CREATE TABLE hTable (rowkey STRING, cf ROW<column1 STRING>, PRIMARY KEY (rowkey) NOT ENFORCED ) WITH (" + hbaseConnection + " )";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //tableEnv.executeSql(kafkaDDLSingleColumn);
        tableEnv.executeSql(kafkaDDLTwoColumns);
        //tableEnv.executeSql(kafkaDDLThreeColumns);
        tableEnv.executeSql(hbaseDDLSingleColumn);


        tableEnv.executeSql("INSERT INTO hTable SELECT rowkey, ROW(column1) FROM kTable");
    }
}

/*
hbase shell
    create 'test_hbase', 'cf'
    scan 'test_hbase'

    a|b -
        ROW                                COLUMN+CELL
         a                                 column=cf:column1, timestamp=1602853529108, value=b
    key|value -
        ROW                                COLUMN+CELL
         a                                 column=cf:column1, timestamp=1602853529108, value=b
         key                               column=cf:column1, timestamp=1602853555153, value=value
    aa|bb
    a|c
        ROW                                COLUMN+CELL
         a                                 column=cf:column1, timestamp=1602897178089, value=c
         aa                                column=cf:column1, timestamp=1602897174078, value=bb
         key                               column=cf:column1, timestamp=1602853555153, value=value
 */