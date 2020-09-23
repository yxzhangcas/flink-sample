package scalalang.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scalalang.util.SensorReading

class SelfCleaningTemperatureAlertFunction(val threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
  private var lastTempState: ValueState[Double] = _
  private var lastTimerState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
    val timestampDescriptor = new ValueStateDescriptor[Long]("timestampState", classOf[Long])
    lastTimerState = getRuntimeContext.getState[Long](timestampDescriptor)
  }
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    val newTimer = ctx.timestamp() + (3600 * 1000)
    val curTimer = lastTimerState.value()
    ctx.timerService().deleteEventTimeTimer(curTimer)
    ctx.timerService().registerEventTimeTimer(newTimer)
    lastTimerState.update(newTimer)

    val lastTemp = lastTempState.value()
    val tempDiff = (value.temperature - lastTemp).abs
    if (tempDiff > threshold) {
      out.collect((value.id, value.temperature, tempDiff))
    }
    this.lastTempState.update(value.temperature)
  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#OnTimerContext, out: Collector[(String, Double, Double)]): Unit = {
    lastTempState.clear()
    lastTimerState.clear()
  }
}
