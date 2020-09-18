import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.RaiseAlertFlatMap
import util.{SensorSource, SensorTimeAssigner, SmokeLevelSource}

object MultiStreamTransformations {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val tempReadings = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    val smokeReadings = env
      .addSource(new SmokeLevelSource)
      .setParallelism(1)

    val keyed = tempReadings
      .keyBy(_.id)
    val alerts = keyed
      .connect(smokeReadings.broadcast)
      .flatMap(new RaiseAlertFlatMap)

    alerts.print()
    env.execute()
  }
}
