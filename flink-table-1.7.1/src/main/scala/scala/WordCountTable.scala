package scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

import scala.util.WC

object WordCountTable {
  def main(args: Array[String]): Unit = {
    //Environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //Source
    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    //Table
    val table = input.toTable(tableEnv)
    //SQL
    //列名表示方法：'NAME
    val filtered = table.groupBy('word).select('word, 'frequency.sum as 'frequency).filter('frequency === 2)
    //DataSet
    val result = filtered.toDataSet[WC]
    //Sink
    result.print()
  }
}
