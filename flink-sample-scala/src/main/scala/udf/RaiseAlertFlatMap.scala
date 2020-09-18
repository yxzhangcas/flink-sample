package udf

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector
import util.{Alert, SensorReading, SmokeLevel}
import util.SmokeLevel.SmokeLevel

class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReading, SmokeLevel, Alert] {
  var smokeLevel = SmokeLevel.Low
  override def flatMap1(in1: SensorReading, collector: Collector[Alert]): Unit = {
    if (smokeLevel.equals(SmokeLevel.High) && in1.temperature > 100) {
      collector.collect(Alert("Risk of fire!", in1.timestamp))
    }
  }
  override def flatMap2(in2: SmokeLevel, collector: Collector[Alert]): Unit = {
    smokeLevel = in2
  }
}
