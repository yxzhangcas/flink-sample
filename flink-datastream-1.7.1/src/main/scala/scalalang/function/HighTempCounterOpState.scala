package scalalang.function

import java.{lang, util}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scalalang.util.SensorReading

class HighTempCounterOpState(val threshold: Double) extends RichFlatMapFunction[SensorReading, (Int, Long)] with ListCheckpointed[java.lang.Long] {
  private lazy val subtaskIdx = getRuntimeContext.getIndexOfThisSubtask
  private var highTempCnt = 0L

  override def flatMap(in: SensorReading, collector: Collector[(Int, Long)]): Unit = {
    if (in.temperature > threshold) {
      highTempCnt += 1
      collector.collect((subtaskIdx, highTempCnt))
    }
  }
  override def restoreState(state: util.List[lang.Long]): Unit = {
    highTempCnt = 0
    for (cnt <- state.asScala) {
      highTempCnt += cnt
    }
  }

  /*
  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[lang.Long] = {
    java.util.Collections.singletonList(highTempCnt)
  }
   */
  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[lang.Long] = {
    val div = highTempCnt / 10
    val mod = (highTempCnt % 10).toInt
    (List.fill(mod)(new lang.Long(div + 1)) ++ List.fill(10 - mod)(new lang.Long(div))).asJava
  }
}
