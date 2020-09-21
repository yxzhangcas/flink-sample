package udf

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.SensorReading

class LateReadingsWindow extends ProcessWindowFunction[SensorReading, (String, Long, Int), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[(String, Long, Int)]): Unit = {
    val cnt = elements.count(_ => true)
    out.collect((key, context.window.getEnd, cnt))
  }
}
