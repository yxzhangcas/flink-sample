import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import udf.async.DerbyAsyncFunction
import udf.function.DerbySyncFunction
import util.{DerbySetup, DerbyWriter, SensorSource, SensorTimeAssigner}

import scala.util.Random

object AsyncFunctionExample {
  def main(args: Array[String]): Unit = {
    DerbySetup.setupDerby(
      """
        |CREATE TABLE SensorLocations (
        | sensor VARCHAR(16) PRIMARY KEY,
        | room VARCHAR(16))
        |""".stripMargin)
    DerbySetup.initializeTable(
      "INSERT INTO SensorLocations (sensor, room) VALUES (?,?)",
      (1 to 80).map(i => Array(s"sensor_$i", s"room_${i % 10}")).toArray.asInstanceOf[Array[Array[Any]]]
    )
    new Thread(new DerbyWriter(
      "UPDATE SensorLocations SET room = ? WHERE sensor = ?",
      (rand: Random) => Array(s"room_${1 + rand.nextInt(20)}", s"sensor_${1 + rand.nextInt(80)}"),
      500L
    )).start()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val readings = env
      .addSource(new SensorSource)
      .setParallelism(8)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
    val sensorLocations = AsyncDataStream.orderedWait(readings, new DerbyAsyncFunction, 5, TimeUnit.SECONDS, 100)
    var sensorLocations1 = readings.map(new DerbySyncFunction)

    sensorLocations.print()
    sensorLocations1.print()
    env.execute()
  }
}
