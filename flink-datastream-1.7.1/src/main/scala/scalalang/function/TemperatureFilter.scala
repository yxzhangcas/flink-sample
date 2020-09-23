package scalalang.function

import org.apache.flink.api.common.functions.FilterFunction

import scalalang.util.SensorReading

class TemperatureFilter(threshold: Long) extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = t.temperature >= threshold
}
