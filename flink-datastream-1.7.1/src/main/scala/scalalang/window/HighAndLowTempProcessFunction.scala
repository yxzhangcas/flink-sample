package scalalang.window

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scalalang.util.{MinMaxTemp, SensorReading}

class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {
    val temps = elements.map(_.temperature)
    val windowEnd = context.window.getEnd
    out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
  }
}
