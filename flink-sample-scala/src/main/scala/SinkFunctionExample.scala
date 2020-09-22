import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.sink.SimpleSocketSink
import util.{SensorSource, SensorTimeAssigner}

object SinkFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    readings
      /*
      向localhost:9191的socket服务发送数据，需要这个服务处于工作状态
      简单的接收回显socket服务：nc -l localhost 9191
       */
      .addSink(new SimpleSocketSink("10.0.30.102", 9191))
      //只有一个线程可以写入
      .setParallelism(1)

    env.execute()
  }
}
