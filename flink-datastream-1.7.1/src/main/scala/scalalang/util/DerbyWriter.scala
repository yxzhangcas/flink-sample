package scalalang.util

import java.sql.DriverManager
import java.util.Properties

import scala.util.Random

class DerbyWriter(stmt: String, paramGenerator: Random => Array[Any], interval: Long) extends Runnable {
  private val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
  private val prepStmt = conn.prepareStatement(stmt)
  private val rand = new Random(1234)

  override def run(): Unit = {
    while (true) {
      Thread.sleep(interval)
      val params = paramGenerator(rand)
      for (i <- 1 to params.length) {
        prepStmt.setObject(i, params(i - 1))
      }
      prepStmt.executeUpdate()
    }
  }
}
