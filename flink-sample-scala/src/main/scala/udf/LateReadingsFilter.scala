package udf

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import util.SensorReading

class LateReadingsFilter(lateReadingsOutput: OutputTag[SensorReading]) extends ProcessFunction[SensorReading, SensorReading] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.timestamp < ctx.timerService().currentWatermark()) {
      ctx.output(lateReadingsOutput, value)
    } else {
      out.collect(value)
    }
  }
}
