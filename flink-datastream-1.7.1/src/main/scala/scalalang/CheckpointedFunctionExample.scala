package scalalang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.function.HighTempCounter
import scalalang.source.SensorSource
import scalalang.timestamp.SensorTimeAssigner

object CheckpointedFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    val highTempCnts = sensorData
      .keyBy(_.id)
      .flatMap(new HighTempCounter(10.0))

    highTempCnts.print()
    env.execute()
  }
}
