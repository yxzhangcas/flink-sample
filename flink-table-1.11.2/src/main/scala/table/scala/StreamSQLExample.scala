package table.scala

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._

object StreamSQLExample {
  case class Order(user: Long, product: String, amount: Int)

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val planner = if (params.has("planner")) params.get("planner") else "blink"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = if (planner == "blink") {
      val settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
      StreamTableEnvironment.create(env, settings)
    } else if (planner == "flink") {
      val settings = EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build()
      StreamTableEnvironment.create(env, settings)
    } else {
      return
    }

    val OrderA = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)
    ))
    val OrderB = env.fromCollection(Seq(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)
    ))

    //表对象，注意列的表示方法
    val tableA = tableEnv.fromDataStream(OrderA, 'user, 'product, 'amount)
    //表视图
    tableEnv.createTemporaryView("OrderB", OrderB, 'user, 'product, 'amount)

    val result = tableEnv.sqlQuery(
      //注意表对象在Scala的SQL语句中的使用方法
      s"""
        |SELECT * FROM $tableA WHERE amount > 2
        |UNION ALL
        |SELECT * FROM OrderB WHERE amount < 2
        |""".stripMargin)

    result.toAppendStream[Order].print()
    env.execute()
  }
}
