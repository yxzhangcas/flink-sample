package udf

import org.apache.flink.api.common.functions.MapFunction
import util.SensorReading

class ProjectionMap extends MapFunction[SensorReading, String] {
  override def map(t: SensorReading): String = t.id
}
