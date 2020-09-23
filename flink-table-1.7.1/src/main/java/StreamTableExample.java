import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import util.Order;

import java.util.Arrays;

public class StreamTableExample {
    public static void main(String[] args) throws Exception {
        //Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        //Source
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));
        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));
        //Table
        Table tableA = tableEnv.fromDataStream(orderA);
        Table tableB = tableEnv.fromDataStream(orderB);
        //SQL
        Table union = tableA.unionAll(tableB).select("user, product, amount").where("amount > 2");
        //DataStream
        DataStream<Order> result = tableEnv.toAppendStream(union, Order.class);
        //Sink
        result.print();
        env.execute();
    }
}
