package scalalang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.function.{ProjectionMap, SplitIdFlatMap, TemperatureFilter}
import scalalang.source.SensorSource
import scalalang.timestamp.SensorTimeAssigner

object BasicTransformations {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val filteredSensors = readings
      //.filter(r => r.temperature >= 25)
      .filter(new TemperatureFilter(25))
    val sensorIds = filteredSensors
      //.map(r => r.id)
      .map(new ProjectionMap)
    val splitIds = sensorIds
      //.flatMap(id => id.split("_"))
      .flatMap(new SplitIdFlatMap)

    splitIds.print()
    env.execute()
  }
}