package scalalang

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.function.UpdatableTemperatureAlertFunction
import scalalang.source.SensorSource
import scalalang.timestamp.SensorTimeAssigner
import scalalang.util.ThresholdUpdate

object BroadcastStateFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val thresholds = env.fromElements(
      ThresholdUpdate("sensor_1", 5.0d),
      ThresholdUpdate("sensor_2", 0.9d),
      ThresholdUpdate("sensor_3", 0.5d),
      ThresholdUpdate("sensor_1", 1.2d),  //更新
      ThresholdUpdate("sensor_3", 0.0d))  //删除

    val keyedSensorData = sensorData
      .keyBy(_.id)

    val broadcastStateDescriptor = new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])
    val broadcastThresholds = thresholds
      .broadcast(broadcastStateDescriptor)

    val alerts = keyedSensorData
      .connect(broadcastThresholds)
      .process(new UpdatableTemperatureAlertFunction)

    alerts.print()
    env.execute()
  }
}