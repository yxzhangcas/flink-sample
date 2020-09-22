package udf.function

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import util.SensorReading

class DerbySyncFunction extends RichMapFunction[SensorReading, (String, String)] {
  var conn: Connection = _
  var query: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    this.conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
    this.query = conn.prepareStatement("SELECT room FROM SensorLocations WHERE sensor = ?")
  }
  override def map(in: SensorReading): (String, String) = {
    val sensor = in.id
    query.setString(1, sensor)
    val result = query.executeQuery()
    val room = if (result.next()) {
      result.getString(1)
    } else {
      "UNKNOWN ROOM"
    }
    result.close()
    Thread.sleep(2000L)
    (sensor, room)
  }
  override def close(): Unit = {
    super.close()
    this.query.close()
    this.conn.close()
  }
}
