import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.TempIncreaseAlertFunction
import util.SensorSource

object ProcessFunctionTimers {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val readings = env
      .addSource(new SensorSource)
    val warnings = readings
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    warnings.print()
    env.execute()
  }
}
