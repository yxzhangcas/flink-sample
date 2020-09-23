package scalalang.timestamp

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

import scalalang.util.SensorReading

class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val bound: Long = 60 * 1000
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.id == "sensor_1") {
      new Watermark(extractedTimestamp - bound)
    } else {
      null
    }
  }
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timestamp
}
