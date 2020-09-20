import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udf.CountFunction;
import udf.OneSecondIntervalTrigger;
import udf.ThirtySecondsWindows;
import util.SensorReading;
import util.SensorSource;
import util.SensorTimeAssigner;

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
