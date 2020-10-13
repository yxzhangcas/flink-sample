package table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class WordCountSQL {
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

        tableEnv.createTemporaryView("WordCount", input, $("word"), $("frequency"));
        Table table = tableEnv.sqlQuery(
                "SELECT word, SUM(frequency) AS frequency FROM WordCount GROUP BY word ORDER BY word");

        DataSet<WC> result = tableEnv.toDataSet(table, WC.class);
        result.print();
    }
}
