package scalalang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.source.SensorSource
import scalalang.timestamp.SensorTimeAssigner

object KeyedTransformations {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val keyed = readings.keyBy(_.id)
    val maxTempPerSensor = keyed.reduce((r1, r2) => {
      if (r1.temperature > r2.temperature) r1 else r2
    })

    maxTempPerSensor.print()
    env.execute()
  }
}
