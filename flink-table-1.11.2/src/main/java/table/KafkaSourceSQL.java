package table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaSourceSQL {
    final static String ddlKafka =
            "CREATE TABLE TestKafka(" +
                    "value1 STRING, " +
                    "value2 STRING " +
                    ") WITH(" +
                    "'connector'='kafka', " +
                    "'topic'='test_kafka', " +
                    "'properties.bootstrap.servers'='10.0.24.124:9092', " +
                    "'properties.group.id'='testGroup', " +
                    "'scan.startup.mode'='latest-offset', " +
                    "'format'='csv', " +
                    "'csv.field-delimiter'='|'" +
                    ")";
    final static String sqlKafka = "SELECT * FROM TestKafka";

    /*
    向标准输出打印内容为：<ID>:8> +I(value1,value2)
    拆解格式：print-identifier:cpu> +I(<values>)
     */
    final static String ddlPrint =
            "CREATE TABLE TestPrint(" +
                    "value1 STRING, " +
                    "value2 STRING" +
                    ") WITH(" +
                    "'connector'='print', " +
                    "'print-identifier'='<ID>', " +
                    "'standard-error'='false'" +
                    ")";

    /*
    基于SQL执行输入输出操作，无需执行env.execute，只有使用了DataStream的操作才需要执行。
     */
    final static String sqlPrint = "INSERT INTO TestPrint SELECT * FROM TestKafka";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql(ddlKafka);
        tableEnv.executeSql(ddlPrint);

        tableEnv.executeSql(sqlPrint);

        Table select = tableEnv.sqlQuery(sqlKafka);
        //此处的类型必须是Row类型
        DataStream<Row> result = tableEnv.toAppendStream(select, Row.class);
        //通过map转换为真正需要的数据类型
        result.map(r -> (String)(r.getField(0))).print();

        env.execute();
    }
}
