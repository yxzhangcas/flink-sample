package table.scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.table.api.bridge.scala._
//导入这个包才能使用'格式的行表示
import org.apache.flink.table.api._

object WordCountTable {
  case class WC(word: String, frequency: Long)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val table = input.toTable(tableEnv)
    val result = table.groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .filter('frequency === 2).toDataSet[WC]

    result.print()
  }
}
