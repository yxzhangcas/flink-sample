package scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

import scala.util.Order

object StreamSQLExample {
  def main(args: Array[String]): Unit = {
    //Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //Source
    val orderA = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)))
    val orderB = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)))
    //Table
    val tableA = tableEnv.fromDataStream(orderA, 'user, 'product, 'amount)
    tableEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount)
    //SQL
    val union = tableEnv.sqlQuery(s"SELECT * FROM $tableA WHERE amount > 2 UNION ALL " +
      "SELECT * FROM OrderB WHERE amount < 2")
    //DataStream
    val result = union.toAppendStream[Order]
    //Sink
    result.print()
    env.execute()
  }
}
