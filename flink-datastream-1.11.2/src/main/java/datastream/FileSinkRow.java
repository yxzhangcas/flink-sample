package datastream;

import datastream.sink.BucketAssignerDay;
import datastream.source.SensorSource;
import datastream.timestamp.SensorTimeAssignerNew;
import datastream.util.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class FileSinkRow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(60 * 1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> input = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SensorTimeAssignerNew()));
                //已经过时
                //.assignTimestampsAndWatermarks(new SensorTimeAssigner(5));

        StreamingFileSink<SensorReading> sink = StreamingFileSink
                .forRowFormat(new Path("."), new SimpleStringEncoder<SensorReading>("UTF-8"))
                .withBucketAssigner(new BucketAssignerDay())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("PREFIX_").withPartSuffix(".SUFFIX").build())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder().withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024).build()
                ).build();

        //input.print();
        input.addSink(sink);
        env.execute();
    }
}
