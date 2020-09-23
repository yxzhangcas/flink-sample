package scalalang

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object RollingSum {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.fromElements((1,2,2),(2,3,1),(2,2,4),(1,5,3))
    val resultStream = inputStream.keyBy(0).sum(1)
    resultStream.print()
    env.execute()
  }
}
