package udf.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import util.SensorReading

class DerbyUpsertSink extends RichSinkFunction[SensorReading] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    val props = new Properties()
    conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", props)
    insertStmt = conn.prepareStatement("INSERT INTO Temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement("UPDATE Temperatures SET temp = ? WHERE sensor = ?")
  }
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
