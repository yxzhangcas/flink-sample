package udf

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import util.SensorReading

class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
  lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0) {
      //副输出
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
    }
    out.collect(value)
  }
}
