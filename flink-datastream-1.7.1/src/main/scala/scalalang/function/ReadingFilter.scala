package scalalang.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scalalang.util.SensorReading

class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
  //lazy方式对应于在Java语言中调用open方法时进行的操作
  lazy val forwardingEnabled: ValueState[Boolean] =
    getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean]))
  lazy val disableTimer: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (forwardingEnabled.value()) {
      out.collect(value)
    }
  }
  override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    forwardingEnabled.update(true)
    val timerTimestamp = ctx.timerService().currentProcessingTime() + value._2
    val curTimerTimestamp = disableTimer.value()
    if (timerTimestamp > curTimerTimestamp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
      disableTimer.update(timerTimestamp)
    }
  }
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
    forwardingEnabled.clear()
    disableTimer.clear()
  }
}
