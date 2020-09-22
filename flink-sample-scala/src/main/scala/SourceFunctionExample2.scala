import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.source.ReplayableCountSource

object SourceFunctionExample2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /*
    可重置数据源需要配合检查点机制进行实现
     */
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    val numbers = env.addSource(new ReplayableCountSource)
    numbers.print()
    env.execute()
  }
}
