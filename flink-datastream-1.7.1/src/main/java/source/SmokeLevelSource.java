package source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import util.SmokeLevel;

import java.util.Random;

public class SmokeLevelSource implements SourceFunction<SmokeLevel> {
    private boolean running = true;
    @Override
    public void run(SourceContext<SmokeLevel> sourceContext) throws Exception {
        Random rand = new Random();
        while (running) {
            if (rand.nextGaussian() > 0.8) {
                sourceContext.collect(SmokeLevel.HIGH);
            } else {
                sourceContext.collect(SmokeLevel.LOW);
            }
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        running = false;
    }
}
