import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.SelfCleaningTemperatureAlertFunction
import util.{SensorSource, SensorTimeAssigner}

object StatefulProcessFunction {
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
      .process(new SelfCleaningTemperatureAlertFunction(1.5))
    alerts.print()
    env.execute()
  }
}
