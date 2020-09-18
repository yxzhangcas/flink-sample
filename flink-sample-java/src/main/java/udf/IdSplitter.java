package udf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class IdSplitter implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] splits = s.split("_");
        for (String split : splits) {
            collector.collect(split);
        }
    }
}
