package scalalang.util

import java.sql.DriverManager
import java.util.Properties

object DerbySetup {
  def setupDerby(tableDDL: String): Unit = {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance()
    val props = new Properties()
    val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample;create=true", props)
    val stmt = conn.createStatement()
    stmt.execute(tableDDL)
    stmt.close()
    conn.close()
  }
  def initializeTable(stmt: String, params: Array[Array[Any]]): Unit = {
    val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
    val prepStmt = conn.prepareStatement(stmt)
    for (stmtParams <- params) {
      for (i <- 1 to stmtParams.length) {
        prepStmt.setObject(i, stmtParams(i - 1))
      }
      prepStmt.addBatch()
    }
    prepStmt.executeBatch()
  }
}
