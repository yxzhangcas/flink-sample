package scalalang.function

import org.apache.flink.api.common.functions.MapFunction

import scalalang.util.SensorReading

class ProjectionMap extends MapFunction[SensorReading, String] {
  override def map(t: SensorReading): String = t.id
}
