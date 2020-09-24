package datastream.source;

import datastream.util.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Instant;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private boolean running = true;
    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random rand = new Random();
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + (taskIdx * 10 + i);
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }
        while (running) {
            long curTime = Instant.now().toEpochMilli();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] += rand.nextGaussian() * 0.5;
                sourceContext.collect(new SensorReading(sensorIds[i], curTime, curFTemp[i]));
            }
            Thread.sleep(100);
        }
    }
    @Override
    public void cancel() {
        this.running = false;
    }
}
