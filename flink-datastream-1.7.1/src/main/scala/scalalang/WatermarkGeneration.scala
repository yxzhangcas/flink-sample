package scalalang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.source.SensorSource
import scalalang.timestamp.{PeriodicAssigner, PunctuatedAssigner}

object WatermarkGeneration {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    //事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置周期生成水位线的间隔，与后面配置的周期性水位线生成器配合使用
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings = env
      .addSource(new SensorSource)
    /*
    算子可以保证发出的水位线是递增且有效的，也就是水位线分配的不合规结果会被丢掉
     */
    //周期水位线，和前面配合
    val readingsWithPeriodicWMs = readings
      .assignTimestampsAndWatermarks(new PeriodicAssigner)
    //触发式水位线，事件触发
    val readingsWithPunctuatedWMs = readings
      .assignTimestampsAndWatermarks(new PunctuatedAssigner)
    //输出
    readingsWithPeriodicWMs.print()
    readingsWithPunctuatedWMs.print()
    env.execute()
  }
}
