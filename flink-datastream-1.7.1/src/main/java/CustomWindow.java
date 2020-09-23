import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.SensorSource;
import timestamp.SensorTimeAssigner;
import util.SensorReading;
import window.CountFunction;
import window.OneSecondIntervalTrigger;
import window.ThirtySecondsWindows;

public class CustomWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(10_000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        DataStream<Tuple4<String, Long, Long, Integer>> countsPerThirtySecs = sensorData
                .keyBy(r -> r.id)
                .window(new ThirtySecondsWindows())
                .trigger(new OneSecondIntervalTrigger())
                .process(new CountFunction());
        countsPerThirtySecs.print();
        env.execute();
    }
}
