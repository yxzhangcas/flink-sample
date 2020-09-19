package udf

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import util.SensorReading

class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
  lazy val lastTemp: ValueState[Double] =
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))
  lazy val currentTimer: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    val prevTemp = lastTemp.value()
    val curTimerTimestamp = currentTimer.value()

    lastTemp.update(value.temperature)
    if (prevTemp == 0.0) {
      //pass
    } else if (value.temperature < prevTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      currentTimer.clear()
    } else if (value.temperature > prevTemp && curTimerTimestamp == 0) {
      val timerTs = ctx.timerService().currentProcessingTime() + 1000
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("Temperature of sensor '" + ctx.getCurrentKey + "' monotonically increased for 1 second.")
    currentTimer.clear()
  }
}
