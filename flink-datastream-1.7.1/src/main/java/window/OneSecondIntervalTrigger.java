package window;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import util.SensorReading;

public class OneSecondIntervalTrigger extends Trigger<SensorReading, TimeWindow> {
    @Override
    public TriggerResult onElement(SensorReading element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Boolean> firstSeen = ctx
                .getPartitionedState(new ValueStateDescriptor<Boolean>("firstSeen", Types.BOOLEAN));
        if (firstSeen.value() == null) {
            long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
            ctx.registerEventTimeTimer(t);
            ctx.registerEventTimeTimer(window.getEnd());
            firstSeen.update(true);
        }
        return TriggerResult.CONTINUE;
    }
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.getEnd()) {
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
            if (t < window.getEnd()) {
                ctx.registerEventTimeTimer(t);
            }
            return TriggerResult.FIRE;
        }
    }
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ValueState<Boolean> firstSeen = ctx
                .getPartitionedState(new ValueStateDescriptor<Boolean>("firstSeen", Types.BOOLEAN));
        firstSeen.clear();
    }
}

