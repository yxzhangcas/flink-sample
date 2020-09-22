import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.sink.DerbyUpsertSink
import util.{DerbyReader, DerbySetup, SensorReading, SensorSource, SensorTimeAssigner}

object IdempotentSinkFunctionExample {
  def main(args: Array[String]): Unit = {
    DerbySetup.setupDerby(
      """
        |CREATE TABLE Temperatures (
        | sensor VARCHAR(16) PRIMARY KEY,
        | temp DOUBLE)
        |""".stripMargin)
    new Thread(new DerbyReader("SELECT sensor, temp FROM Temperatures ORDER BY sensor", 10 * 1000))
      .start()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    val celsiusReadings = sensorData
      .map(r => SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))

    celsiusReadings.addSink(new DerbyUpsertSink)

    env.execute()
  }
}
