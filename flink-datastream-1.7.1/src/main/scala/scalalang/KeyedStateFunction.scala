package scalalang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.function.TemperatureAlertFunction
import scalalang.source.SensorSource
import scalalang.timestamp.SensorTimeAssigner

object KeyedStateFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val keyedSensorData = sensorData
      .keyBy(_.id)

    val alerts = keyedSensorData
      .flatMap(new TemperatureAlertFunction(1.7))

    //一种快速的实现方法，得先熟悉天Scala的相关语法才能看懂
    /*
    此实现的考虑比较全面：
      1. 当状态不存在时，创建和更新状态，不产生告警
      2. 当状态存在时，比较温度差异，更新状态，产生告警
     */
    /*
    val alerts = keyedSensorData
      .flatMapWithState[(String, Double, Double), Double]{
        case (in: SensorReading, None) =>
          (List.empty, Some(in.temperature))
        case (r: SensorReading, lastTemp: Some[Double]) =>
          val tempDiff = (r.temperature - lastTemp.get).abs
          if (tempDiff > 1.7) {
            (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
          } else {
            (List.empty, Some(r.temperature))
          }
      }
     */

    alerts.print()
    env.execute()
  }
}
