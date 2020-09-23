package scalalang.window

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scalalang.util.SensorReading

class UpdatingWindowCountFunction extends ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[(String, Long, Int, String)]): Unit = {
    val cnt = elements.count(_ => true)
    val isUpdate = context.windowState.getState(new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean]))
    if (!isUpdate.value()) {
      out.collect((key, context.window.getEnd, cnt, "first"))
      isUpdate.update(true)
    } else {
      out.collect((key, context.window.getEnd, cnt, "update"))
    }
  }
}
