import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import udf.{AssignWindowEndProcessFunction, AvgTempFunction, HighAndLowTempProcessFunction, MinTempFunction}
import util.{SensorSource, SensorTimeAssigner}

object WindowFunctions {
  def threshold = 25.0

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val minTempPerWindow = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

    val minTempPerWindow2 = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce(new MinTempFunction)

    val avgTempPerWindow = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .aggregate(new AvgTempFunction)

    val minMaxTempPerWindow = sensorData
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempProcessFunction)

    val minMaxTempPerWindow2 = sensorData
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2), r1._3.max(r2._3)), new AssignWindowEndProcessFunction)

    minMaxTempPerWindow2.print()
    env.execute()
  }
}
