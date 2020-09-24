package datastream.sink;

import datastream.util.SensorReading;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class BucketAssignerDay implements BucketAssigner<SensorReading, String> {
    @Override
    public String getBucketId(SensorReading sensorReading, Context context) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(sensorReading.timestamp), ZoneId.of("Asia/Shanghai"))
                .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
