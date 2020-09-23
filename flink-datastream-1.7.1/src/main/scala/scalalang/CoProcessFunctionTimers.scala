package scalalang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.function.ReadingFilter
import scalalang.source.SensorSource

object CoProcessFunctionTimers {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //两条输入流
    val filterSwitches = env
      .fromCollection(Seq(("sensor_2", 10 * 1000L), ("sensor_7", 60 * 1000L)))
    val readings = env
      .addSource(new SensorSource)
    //关联、键值、处理
    val forwardedReadings = readings
      .connect(filterSwitches)
      .keyBy(_.id, _._1)
      .process(new ReadingFilter)
    //输出流
    forwardedReadings.print()
    env.execute()
  }
}
