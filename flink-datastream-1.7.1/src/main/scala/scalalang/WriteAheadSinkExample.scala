package scalalang

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import scalalang.function.FailingMapper
import scalalang.sink.StdOutWriteAheadSink
import scalalang.source.ResettableSensorSource
import scalalang.timestamp.SensorTimeAssigner

object WriteAheadSinkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData = env
      .addSource(new ResettableSensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    val avgTemp = sensorData
      .timeWindowAll(Time.seconds(1))
      .apply((w, vals, out: Collector[(String, Double)]) => {
        val avgTemp = vals.map(_.temperature).sum / vals.count(_ => true)
        val epochSeconds = w.getEnd / 1000
        val tString = LocalDateTime
          .ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC)
          .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        out.collect((tString, avgTemp))
      })
      //产生异常触发任务的故障恢复
      .map(new FailingMapper[(String, Double)](16))
      .setParallelism(1)

    avgTemp
      .transform("WriteAheadSink", new StdOutWriteAheadSink)
      .setParallelism(1)
    avgTemp
      .print()
      .setParallelism(1)

    env.execute()
  }
}
