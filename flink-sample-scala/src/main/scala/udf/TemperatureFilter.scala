package udf

import org.apache.flink.api.common.functions.FilterFunction
import util.SensorReading

class TemperatureFilter(threshold: Long) extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = t.temperature >= threshold
}
