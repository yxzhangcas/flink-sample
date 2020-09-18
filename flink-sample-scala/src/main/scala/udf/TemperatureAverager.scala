package udf

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.SensorReading

class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[SensorReading], out: Collector[SensorReading]): Unit = {
    val (cnt, sum) = input.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt
    out.collect(SensorReading(key, window.getEnd, avgTemp))
  }
}
