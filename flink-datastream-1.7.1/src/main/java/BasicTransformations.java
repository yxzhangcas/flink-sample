import function.IdExtractor;
import function.IdSplitter;
import function.TemperatureFilter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.SensorSource;
import timestamp.SensorTimeAssigner;
import util.SensorReading;

public class BasicTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> readings = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<SensorReading> filteredReadings = readings
                .filter(new TemperatureFilter(25));
        //.filter(r -> r.temperature >= 25);

        DataStream<String> sensorIds = readings
                .map(new IdExtractor());
        //.map(r -> r.id);

        DataStream<String> splitIds = sensorIds
                .flatMap(new IdSplitter());
                /*
                //需要添加类型声明，避免Lambda的歧义
                .flatMap((FlatMapFunction<String, String>)(id, out) -> {
                    for (String s : id.split("_")) {
                        out.collect(s);
                    }
                })
                //Java语言是静态类型，需要明确指定Lambda的返回值类型
                .returns(Types.STRING);
                 */

        splitIds.print();
        env.execute();
    }
}
