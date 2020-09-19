import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.FreezingMonitor
import util.{SensorSource, SensorTimeAssigner}

object SideOutputs {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //检查点生成间隔
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    //事件时间，周期水位线
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    val monitoredReadings = readings
      .process(new FreezingMonitor)
    //副输出
    monitoredReadings
      .getSideOutput(new OutputTag[String]("freezing-alarms"))
      .print()
    //输出
    readings.print()
    env.execute()
  }
}
