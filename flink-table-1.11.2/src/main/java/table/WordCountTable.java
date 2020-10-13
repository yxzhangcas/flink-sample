package table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

//这个符号需要额外包含进来，让Java语言的表现力赶上Scala语言
import static org.apache.flink.table.api.Expressions.$;

public class WordCountTable {
    public static class WC {
        public String word;
        public long frequency;
        public WC() {}
        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }
        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", frequency=" + frequency +
                    '}';
        }
    }
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Hello", 1));

        //Flink Planner Batch
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        /*
        Blink将批处理作业，视为流式处理的特殊情况。所以，blink不支持表和DataSet之间的转换，
        批处理作业将不转换为DataSet应用程序，而是跟流处理一样，转换为DataStream程序来处理。
        Blink planner也不支持BatchTableSource，而使用有界的StreamTableSource代替。
        无论Table API和SQL的输入是流数据还是批数据，都将转换为DataStream程序
         */

        Table table = tableEnv.fromDataSet(input);
        Table filtered = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isEqual(2));

        DataSet<WC> result = tableEnv.toDataSet(filtered, WC.class);
        result.print();
    }
}
