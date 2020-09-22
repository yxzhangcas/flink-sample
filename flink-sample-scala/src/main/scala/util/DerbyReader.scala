package util

import java.sql.DriverManager
import java.util.Properties

class DerbyReader(query: String, interval: Long) extends Runnable {
  private val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
  private val prepStmt = conn.prepareStatement(query)
  private val numResultCols = prepStmt.getMetaData.getColumnCount

  override def run(): Unit = {
    val cols = new Array[Any](numResultCols)
    while (true) {
      Thread.sleep(interval)
      val res = prepStmt.executeQuery()
      while (res.next()) {
        for (i <- 1 to numResultCols) {
          cols(i - 1) = res.getObject(i)
        }
        println(s"${cols.mkString(", ")}")
      }
      res.close()
    }
  }
}
