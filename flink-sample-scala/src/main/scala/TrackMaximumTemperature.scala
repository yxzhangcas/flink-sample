import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import util.{SensorSource, SensorTimeAssigner}

object TrackMaximumTemperature {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    val tenSecsMaxTemps = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      //每10秒内的最大值
      .timeWindow(Time.seconds(1))
      .max(1)
    tenSecsMaxTemps
      .keyBy(_._1)
      .asQueryableState("maxTemperature")

    //sensorData.print()
    env.execute()
  }
}
