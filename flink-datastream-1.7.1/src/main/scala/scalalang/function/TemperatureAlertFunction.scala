package scalalang.function

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scalalang.util.SensorReading

class TemperatureAlertFunction(val threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  private var lastTempState: ValueState[Double] = _
  override def open(parameters: Configuration): Unit = {
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
  }
  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    val tempDiff = (in.temperature - lastTemp).abs
    //只输出相邻温度的跳变超阈值的数据，即只输出告警信息
    //这里存在一个问题，open时创建的状态里面是没有参考值的，默认是0
    //第一个数据来的时候会触发告警，但实际并不是真正的告警
    if (tempDiff > threshold) {
      collector.collect((in.id, in.temperature, tempDiff))
    }
    this.lastTempState.update(in.temperature)
  }
}
