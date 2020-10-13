package table;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;

public class StreamSQLExample {
    public static class Order {
        public Long user;
        public String product;
        public int amount;
        public Order() {}
        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }
        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv;
        if (Objects.equals(planner, "blink")) {
            EnvironmentSettings settings = EnvironmentSettings
                    .newInstance().inStreamingMode().useBlinkPlanner().build();
            tableEnv = StreamTableEnvironment.create(env, settings);
            System.out.println("Blink");
        } else if (Objects.equals(planner, "flink")) {
            EnvironmentSettings settings = EnvironmentSettings
                    .newInstance().inStreamingMode().useOldPlanner().build();
            tableEnv = StreamTableEnvironment.create(env, settings);
            System.out.println("Flink");
        } else {
            return;
        }

        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));
        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        //表对象，注意列的表示方法
        Table tableA = tableEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        //表视图
        tableEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));

        Table union = tableEnv.sqlQuery(
                //注意表对象在Java的SQL语句中的使用方法
                "SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
                "SELECT * FROM OrderB WHERE amount < 2");

        DataStream<Order> result = tableEnv.toAppendStream(union, Order.class);
        result.print();
        env.execute();
    }
}
