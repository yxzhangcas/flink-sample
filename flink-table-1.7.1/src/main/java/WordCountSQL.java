import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import util.WC;

public class WordCountSQL {
    public static void main(String[] args) throws Exception {
        //Environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        //Source
        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Hello", 1));
        //Table
        tableEnv.registerDataSet("WordCount", input, "word, frequency");
        //SQL
        Table table = tableEnv.sqlQuery("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word ORDER BY word");
        //DataSet
        DataSet<WC> result = tableEnv.toDataSet(table, WC.class);
        //Sink
        result.print();
    }
}
