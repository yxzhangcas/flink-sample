package function;

import org.apache.flink.api.common.functions.FilterFunction;
import util.SensorReading;

public class TemperatureFilter implements FilterFunction<SensorReading> {
    private final double threshold;
    public TemperatureFilter(double threshold) {
        this.threshold = threshold;
    }
    @Override
    public boolean filter(SensorReading sensorReading) throws Exception {
        return sensorReading.temperature >= threshold;
    }
}
