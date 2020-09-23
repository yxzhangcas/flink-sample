package scalalang.window

import java.util
import java.util.Collections

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class ThirtySecondsWindows extends WindowAssigner[Object, TimeWindow] {
  val windowSize: Long = 30 * 1000L
  override def assignWindows(element: Object, timestamp: Long, context: WindowAssigner.WindowAssignerContext): util.Collection[TimeWindow] = {
    val startTime = timestamp - (timestamp % windowSize)
    val endTime = startTime + windowSize
    Collections.singletonList(new TimeWindow(startTime, endTime))
  }
  override def getDefaultTrigger(env: StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
    EventTimeTrigger.create()
  }
  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
    new TimeWindow.Serializer
  }
  override def isEventTime: Boolean = true
}
