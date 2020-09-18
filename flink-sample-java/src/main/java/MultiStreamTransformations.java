import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udf.RaiseAlertFlatMap;
import util.*;

public class MultiStreamTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> tempReadings = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        DataStream<SmokeLevel> smokeReadings = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1);

        KeyedStream<SensorReading, String> keyedTempReadings = tempReadings
                .keyBy(r -> r.id);
        DataStream<Alert> alerts = keyedTempReadings
                //将第二条输入流广播，保持与第一条流的关联
                .connect(smokeReadings.broadcast())
                .flatMap(new RaiseAlertFlatMap());

        alerts.print();
        env.execute();
    }
}
