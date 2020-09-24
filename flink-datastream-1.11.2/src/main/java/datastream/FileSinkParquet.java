package datastream;

import datastream.source.SensorSource;
import datastream.timestamp.SensorTimeAssignerNew;
import datastream.util.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.time.Duration;

public class FileSinkParquet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //检查点生成周期
        env.enableCheckpointing(60 * 1000);
        //使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //水位线自动发送间隔
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> input = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                //泛型方法，需要明确指定对象成员的类型；水位线支持的乱序时间
                                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                //分配时间时间戳，对于已有时间戳的数据不需要再分配
                                .withTimestampAssigner(new SensorTimeAssignerNew()));
                //已经过时
                //.assignTimestampsAndWatermarks(new SensorTimeAssigner(10));

        StreamingFileSink<SensorReading> sink = StreamingFileSink
                .forBulkFormat(new Path("."), ParquetAvroWriters.forReflectRecord(SensorReading.class))
                //默认：OutputFileConfig.builder().build()
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("prefix_").withPartSuffix(".suffix").build())
                //默认：yyyy-MM-dd--HH
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyyMMdd"))
                //默认：DEFAULT_BUCKET_CHECK_INTERVAL
                .withBucketCheckInterval(60 * 1000L)
                //默认：OnCheckpointRollingPolicy.build()
                /*
                Bulk Formats can only have `OnCheckpointRollingPolicy`, which rolls (ONLY) on every checkpoint.
                When using Hadoop < 2.7, please use the OnCheckpointRollingPolicy which rolls part files on every checkpoint.
                The reason is that if part files “traverse” the checkpoint interval,
                then, upon recovery from a failure the StreamingFileSink may use the truncate() method of
                the filesystem to discard uncommitted data from the in-progress file.
                This method is not supported by pre-2.7 Hadoop versions and Flink will throw an exception.
                 */
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        //input.print();
        input.addSink(sink);
        env.execute();
    }
}