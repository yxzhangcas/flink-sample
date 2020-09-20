import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udf.ReadingFilter;
import util.SensorReading;
import util.SensorSource;

public class CoProcessFunctionTimers {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple2<String, Long>> filterSwitchs = env
                .fromElements(Tuple2.of("sensor_2", 10_000L), Tuple2.of("sensor_7", 60_000L));
        DataStream<SensorReading> readings = env.addSource(new SensorSource());

        DataStream<SensorReading> forwardedReadings = readings
                .connect(filterSwitchs)
                .keyBy(r -> r.id, s -> s.f0)
                .process(new ReadingFilter());

        forwardedReadings.print();
        env.execute();
    }
}
