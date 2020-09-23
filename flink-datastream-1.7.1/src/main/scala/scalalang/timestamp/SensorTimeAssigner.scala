package scalalang.timestamp

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

import scalalang.util.SensorReading

class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
  override def extractTimestamp(t: SensorReading): Long = t.timestamp
}
