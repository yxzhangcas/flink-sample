package table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaSourceJson {
    final static String kafkaConnection = "'connector'='kafka', 'topic'='test_kafka', " +
            "'properties.bootstrap.servers'='10.0.24.124:9092', " +
            "'properties.group.id'='testGroup', " +
            "'scan.startup.mode'='latest-offset', " +
            "'format'='json', 'json.fail-on-missing-field'='false', 'json.ignore-parse-errors'='true', " +
            "'json.timestamp-format.standard'='SQL'";
            //"'json.timestamp-format.standard'='ISO-8601'";

    final static String kafkaDDLSingleColumn = "CREATE TABLE kTable ( column1 STRING ) WITH (" + kafkaConnection + " )";
    final static String kafkaDDLMultipleColumn = "CREATE TABLE kTable ( column1 STRING, column2 STRING ) WITH (" + kafkaConnection + " )";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //tableEnv.executeSql(kafkaDDLSingleColumn);
        tableEnv.executeSql(kafkaDDLMultipleColumn);
        tableEnv.toAppendStream(tableEnv.sqlQuery("SELECT * FROM kTable"), Row.class).print();
        env.execute();
    }
}

/*
kafka-console-producer.sh --broker-list localhost:9092 --topic test_kafka
1. kafkaDDLSingleColumn
    空格回车            - 无输出
    {}                  - null
    {a}                 - 无输出，非法格式
    {"a":"b"}           - null
    {"column1":"a"}     - a
    {"column1":"a", "column2":"b"}  - a #忽略冗余字段
2. kafkaDDLMultipleColumn
    空格回车            - 无输出
    {}                  - null,null
    {a}                 - 无输出，非法格式
    {"a":"b"}           - null,null
    {"column1":"a"}     - a,null
    {"column2":"a"}     - null,a
    {"column1":"a", "a":"b"}  - a,null #忽略冗余字段
    {"column1":"a", "column2":"b"}  - a,b
 */