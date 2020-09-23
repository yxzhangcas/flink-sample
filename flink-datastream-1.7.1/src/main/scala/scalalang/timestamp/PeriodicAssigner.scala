package scalalang.timestamp

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

import scalalang.util.SensorReading

class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound: Long = 60 * 1000
  var maxTs: Long = Long.MinValue
  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound);
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    element.timestamp
  }
}
