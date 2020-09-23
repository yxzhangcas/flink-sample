package function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import util.SensorReading;

public class ReadingFilter extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {
    private ValueState<Boolean> forwardingEnabled;
    private ValueState<Long> disableTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
        forwardingEnabled = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("filterSwitch", Types.BOOLEAN));
        disableTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
    }
    @Override
    public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
        Boolean forward = forwardingEnabled.value();
        if (forward != null && forward) {
            out.collect(value);
        }
    }
    @Override
    public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
        forwardingEnabled.update(true);
        long timerTimestamp = ctx.timerService().currentProcessingTime() + value.f1;
        Long curTimerTimestamp = disableTimer.value();
        if (curTimerTimestamp == null || timerTimestamp > curTimerTimestamp) {
            if (curTimerTimestamp != null) {
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
            }
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            disableTimer.update(timerTimestamp);
        }
    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
        forwardingEnabled.clear();
        disableTimer.clear();
    }
}

