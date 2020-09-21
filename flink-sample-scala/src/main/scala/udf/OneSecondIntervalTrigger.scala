package udf

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import util.SensorReading

class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
  override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val firstSeen = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))
    if (!firstSeen.value()) {
      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      ctx.registerEventTimeTimer(t)
      ctx.registerEventTimeTimer(window.getEnd)
      firstSeen.update(true)
    }
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time == window.getEnd) {
      TriggerResult.FIRE_AND_PURGE
    } else {
      val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      if (t < window.getEnd) {
        ctx.registerEventTimeTimer(t)
      }
      TriggerResult.FIRE
    }
  }
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    val firstSeen = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))
    firstSeen.clear()
  }
}
