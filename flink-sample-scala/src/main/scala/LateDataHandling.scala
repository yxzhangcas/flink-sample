import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import udf.{LateReadingsFilter, LateReadingsWindow, TimestampShuffler, UpdatingWindowCountFunction}
import util.{SensorReading, SensorSource, SensorTimeAssigner}

object LateDataHandling {
  val lateReadingsOutput = new OutputTag[SensorReading]("late-readings")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(500L)

    val outOfOrderReadings = env
      .addSource(new SensorSource)
      .map(new TimestampShuffler(7 * 1000))
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    //方法1：迟到数据过滤到副输出（处理函数中操作）
    //filterLateReading(outOfOrderReadings)
    //方法2：迟到数据窗口重定向到副输出
    //sideOutputLateEventsWindow(outOfOrderReadings)
    //方法3：迟到数据更新窗口结果
    updateForLateEventsWindow(outOfOrderReadings)

    env.execute()
  }

  def filterLateReading(readings: DataStream[SensorReading]): Unit = {
    val filteredReadings = readings.process(new LateReadingsFilter(lateReadingsOutput))
    val lateReadings = filteredReadings.getSideOutput(lateReadingsOutput)
    filteredReadings
      .print()
    lateReadings
      .map(r => "*** late reading ***" + r.id)
      .print()
  }
  def sideOutputLateEventsWindow(readings: DataStream[SensorReading]): Unit = {
    val countPer10Secs = readings
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .sideOutputLateData(lateReadingsOutput)
      .process(new LateReadingsWindow)
    countPer10Secs
      .getSideOutput(lateReadingsOutput)
      .map(r => "*** late reading ***" + r.id)
      .print()
    countPer10Secs
      .print()
  }
  def updateForLateEventsWindow(readings: DataStream[SensorReading]): Unit = {
    val countPer10Secs = readings
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .allowedLateness(Time.seconds(5))
      .process(new UpdatingWindowCountFunction)
    countPer10Secs.print()
  }
}
