package scalalang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import scalalang.source.SensorSource
import scalalang.timestamp.SensorTimeAssigner
import scalalang.util.SensorReading
import scalalang.window.TemperatureAverager

object AverageSensorReadings {
  def main(args: Array[String]): Unit = {
    //执行环境：使用事件时间
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //显式指定本地或远程执行环境
    /*
    val localEnv = StreamExecutionEnvironment.createLocalEnvironment()
    val remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment("host", 1234, "path/to/jarFiles")
     */

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //水位线
    env.getConfig.setAutoWatermarkInterval(1000L)
    //数据源：分配时间戳和水位线
    val sensorData = env
      .addSource(new SensorSource)
      //时间戳
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    //数据处理
    val avgTemp = sensorData
      //映射
      .map(r => {
        val celsius = (r.temperature - 32) * (5.0 / 9.0)
        SensorReading(r.id, r.timestamp, celsius)
      })
      //分组
      .keyBy(_.id)
      //窗口
      .timeWindow(Time.seconds(1))
      //UDF
      .apply(new TemperatureAverager)
    //数据汇
    avgTemp.print()
    //执行
    env.execute()
  }
}
