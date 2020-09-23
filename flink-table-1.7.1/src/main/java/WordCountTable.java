import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import util.WC;

public class WordCountTable {
    public static void main(String[] args) throws Exception {
        //问题：CollectionsEnvironment是什么环境？
        //Environment
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        //Source
        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("Ciao", 1),
                new WC("Hello", 1));
        //Table
        Table table = tableEnv.fromDataSet(input);
        //SQL
        Table filtered = table.groupBy("word").select("word, frequency.sum as frequency").orderBy("word");//.filter("frequency = 2");
        //DataSet
        DataSet<WC> result = tableEnv.toDataSet(filtered, WC.class);
        //Sink
        result.print();
    }
}
