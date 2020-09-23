package scalalang.function

import org.apache.flink.api.common.functions.MapFunction
import scalalang.util.SensorReading

import scala.util.Random

class TimestampShuffler(maxRandomOffset: Int) extends MapFunction[SensorReading, SensorReading] {
  lazy val rand = new Random()
  override def map(t: SensorReading): SensorReading = {
    val shuffleTs = t.timestamp + rand.nextInt(maxRandomOffset)
    SensorReading(t.id, shuffleTs, t.temperature)
  }
}
