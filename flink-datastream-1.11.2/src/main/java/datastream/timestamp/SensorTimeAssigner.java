package datastream.timestamp;

import datastream.util.SensorReading;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {
    public SensorTimeAssigner(long maxDelayInSecs) {
        super(Time.seconds(maxDelayInSecs));
    }
    @Override
    public long extractTimestamp(SensorReading sensorReading) {
        return sensorReading.timestamp;
    }
}
