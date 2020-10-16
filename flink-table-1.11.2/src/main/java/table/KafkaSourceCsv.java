package table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaSourceCsv {
    final static String kafkaConnection = "'connector'='kafka', 'topic'='test_kafka', " +
            "'properties.bootstrap.servers'='10.0.24.124:9092', " +
            "'properties.group.id'='testGroup', " +
            "'scan.startup.mode'='latest-offset', " +
            "'format'='csv', 'csv.field-delimiter'='|', " +
            "'csv.line-delimiter'=U&'\\000A', 'csv.ignore-parse-errors'='true'";
    final static String kafkaDDLSingleColumn = "CREATE TABLE kTable ( column1 STRING ) WITH (" + kafkaConnection + " )";
    final static String kafkaDDLTwoColumns = "CREATE TABLE kTable ( column1 STRING, column2 STRING ) WITH (" + kafkaConnection + " )";
    final static String kafkaDDLThreeColumns = "CREATE TABLE kTable ( column1 STRING, column2 STRING, column3 STRING ) WITH (" + kafkaConnection + " )";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql(kafkaDDLSingleColumn);
        //tableEnv.executeSql(kafkaDDLTwoColumns);
        //tableEnv.executeSql(kafkaDDLThreeColumns);
        tableEnv.toAppendStream(tableEnv.sqlQuery("SELECT * FROM kTable"), Row.class).print();
        env.execute();
    }
}

/*
kafka-console-producer.sh --broker-list localhost:9092 --topic test_kafka

输入输出对应情况：
1. kafkaDDLSingleColumn：
    空行回车 - 无输出
    无|内容 - 原样输出     # a -> a
    带|内容 - 无输出      # 特例：a| -> a
2. kafkaDDLTwoColumns
    空行回车 - 无输出
    无|内容 - 原样,null              # a -> a,null(null)
    单|内容 - 原样输出(逗号分隔)       # a|b -> a,b    a| -> a,(空串非null)   |a -> ,a(空串)    | -> ,(两个空串)
    多|内容 - 无输出(例外：最后一个字符是|时可以忽略，)
 */