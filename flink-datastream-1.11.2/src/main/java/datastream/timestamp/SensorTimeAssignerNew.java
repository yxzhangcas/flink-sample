package datastream.timestamp;

import datastream.util.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class SensorTimeAssignerNew implements SerializableTimestampAssigner<SensorReading> {
    @Override
    public long extractTimestamp(SensorReading sensorReading, long l) {
        return sensorReading.timestamp;
    }
}
