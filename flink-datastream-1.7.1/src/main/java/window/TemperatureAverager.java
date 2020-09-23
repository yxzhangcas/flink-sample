package window;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.SensorReading;

public class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<SensorReading> collector) throws Exception {
        int cnt = 0;
        double sum = 0.0;
        for (SensorReading r : iterable) {
            cnt++;
            sum += r.temperature;
        }
        double avgTemp = sum / cnt;
        collector.collect(new SensorReading(s, timeWindow.getEnd(), avgTemp));
    }
}
