import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import source.SensorSource;
import timestamp.SensorTimeAssigner;
import util.SensorReading;
import window.TemperatureAverager;

public class AverageSensorReadings {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //显式指定本地或远程执行环境
        /*
        StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment("host", 1234, "path/to/jarFiles");
         */
        //事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //水位线
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env
                //数据源
                .addSource(new SensorSource())
                //时间戳
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<SensorReading> avgTemp = sensorData
                //映射
                .map(r -> new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
                //分组
                .keyBy(r -> r.id)
                //窗口
                .timeWindow(Time.seconds(1))
                //UDF
                .apply(new TemperatureAverager());
        //数据汇
        avgTemp.print();
        //执行
        env.execute();
    }
}
