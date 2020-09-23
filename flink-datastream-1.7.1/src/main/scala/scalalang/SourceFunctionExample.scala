package scalalang

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scalalang.source.CountSource

object SourceFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.addSource(new CountSource)
    numbers.print()
    env.execute()
  }
}
