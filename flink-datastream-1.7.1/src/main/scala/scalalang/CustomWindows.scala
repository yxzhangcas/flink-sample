package scalalang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.source.SensorSource
import scalalang.timestamp.SensorTimeAssigner
import scalalang.window.{CountFunction, OneSecondIntervalTrigger, ThirtySecondsWindows}

object CustomWindows {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val countsPerThirtySecs = sensorData
      .keyBy(_.id)
      .window(new ThirtySecondsWindows)
      .trigger(new OneSecondIntervalTrigger)
      .process(new CountFunction)

    countsPerThirtySecs.print()
    env.execute()
  }
}