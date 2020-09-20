package udf;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.SensorReading;

public class CountFunction extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, Integer>, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<SensorReading> elements, Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
        int cnt = 0;
        for (SensorReading r : elements) {
            cnt++;
        }
        long evalTime = context.currentWatermark();
        out.collect(Tuple4.of(s, context.window().getEnd(), evalTime, cnt));
    }
}
