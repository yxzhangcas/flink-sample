package udf;

import org.apache.flink.api.common.functions.MapFunction;
import util.SensorReading;

public class IdExtractor implements MapFunction<SensorReading, String> {
    @Override
    public String map(SensorReading sensorReading) throws Exception {
        return sensorReading.id;
    }
}
