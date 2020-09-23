package scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

import scala.util.WC

object WordCountSQL {
  def main(args: Array[String]): Unit = {
    //Environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //Source
    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    //Table
    tableEnv.registerDataSet("WordCount", input, 'word, 'frequency)
    //SQL
    val table = tableEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")
    //DataSet
    val result = table.toDataSet[WC]
    //Sink
    result.print()
  }
}
