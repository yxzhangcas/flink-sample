import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.SensorReading;
import util.SensorSource;
import util.SensorTimeAssigner;

public class KeyedTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> readings = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        KeyedStream<SensorReading, String> keyed = readings.keyBy(r -> r.id);
        DataStream<SensorReading> maxTempPerSensor = keyed
                .reduce((r1, r2) -> {
                    if (r1.temperature > r2.temperature) {
                        return r1;
                    } else {
                        return r2;
                    }
                });
        maxTempPerSensor.print();
        env.execute();
    }
}
