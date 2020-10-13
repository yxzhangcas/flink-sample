package table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

public class StreamWindowSQLExample {
    private static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("orders", ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String contents = "1,beer,3,2019-12-12 00:00:01\n" +
                "1,diaper,4,2019-12-12 00:00:02\n" +
                "2,pen,3,2019-12-12 00:00:04\n" +
                "2,rubber,3,2019-12-12 00:00:06\n" +
                "3,rubber,2,2019-12-12 00:00:05\n" +
                "4,beer,1,2019-12-12 00:00:08";
        String path = createTempFile(contents);

        String ddl = "CREATE TABLE orders (\n" +
                "  user_id INT,\n" +
                "  product STRING,\n" +
                "  amount INT,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',\n" +
                "  'connector.path' = '" + path + "',\n" +
                "  'format.type' = 'csv'\n" +
                ")";
        tableEnv.executeSql(ddl);

        String query = "SELECT\n" +
                "  CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start,\n" +
                "  COUNT(*) order_num,\n" +
                "  SUM(amount) total_amount,\n" +
                "  COUNT(DISTINCT product) unique_products\n" +
                "FROM orders\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";
        Table result = tableEnv.sqlQuery(query);

        tableEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }
}
