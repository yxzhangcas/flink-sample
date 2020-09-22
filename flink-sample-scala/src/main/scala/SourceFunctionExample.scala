import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.source.CountSource

object SourceFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.addSource(new CountSource)
    numbers.print()
    env.execute()
  }
}