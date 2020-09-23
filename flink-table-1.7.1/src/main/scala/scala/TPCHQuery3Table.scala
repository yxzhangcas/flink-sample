package scala

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object TPCHQuery3Table {
  /*
  USAGE: TPCHQuery3Expression <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>
   */
  def main(args: Array[String]): Unit = {
    if (!parseParameters(args)) {
      return
    }
    val date = "1995-03-12".toDate
    //Environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //Source-Table-SQL
    val lineItems = getLineItemDataSet(env)
      .toTable(tableEnv, 'id, 'extdPrice, 'discount, 'shipDate)
      .filter('shipDate.toDate > date)
    val customers = getCustomerDataSet(env)
      .toTable(tableEnv, 'id, 'mktSegment)
      .filter('mktSegment === "AUTOMOBILE")
    val orders = getOrdersDataSet(env)
      .toTable(tableEnv, 'orderId, 'custId, 'orderDate, 'shipPrio)
      .filter('orderDate.toDate < date)
    //SQL
    val items = orders.join(customers).where('custId === 'id).select('orderId, 'orderDate, 'shipPrio)
      .join(lineItems).where('orderId === 'id)
      .select('orderId, 'extdPrice * (1.0f.toExpr - 'discount) as 'revenue, 'orderDate, 'shipPrio)
    val result = items.groupBy('orderId, 'orderDate, 'shipPrio)
      .select('orderId, 'revenue.sum as 'revenue, 'orderDate, 'shipPrio).orderBy('revenue.desc, 'orderDate.desc)
    //DataSet-Sink
    result.writeAsCsv(outputPath, "\n", "|")
    env.execute()
  }
  private var lineItemPath: String = _
  private var customerPath: String = _
  private var ordersPath: String = _
  private var outputPath: String = _
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 4) {
      lineItemPath = args(0)
      customerPath = args(1)
      ordersPath = args(2)
      outputPath = args(3)
      true
    } else {
      false
    }
  }
  case class LineItem(id: Long, extdPrice: Double, discount: Double, shipDate: String)
  private def getLineItemDataSet(env: ExecutionEnvironment): DataSet[LineItem] = {
    env.readCsvFile[LineItem](lineItemPath, fieldDelimiter = "|", includedFields = Array(0, 5, 6, 10))
  }
  case class Customer(id: Long, mktSegment: String)
  private def getCustomerDataSet(env: ExecutionEnvironment): DataSet[Customer] = {
    env.readCsvFile[Customer](customerPath, fieldDelimiter = "|", includedFields = Array(0, 6))
  }
  case class Order(orderId: Long, custId: Long, orderDate: String, shipPrio: Long)
  private def getOrdersDataSet(env: ExecutionEnvironment): DataSet[Order] = {
    env.readCsvFile[Order](ordersPath, fieldDelimiter = "|", includedFields = Array(0, 1, 4, 7))
  }
}
