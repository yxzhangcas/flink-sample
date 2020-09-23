package scalalang.function

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scalalang.util.SensorReading

class HighTempCounter(val threshold: Double) extends FlatMapFunction[SensorReading, (String, Long, Long)] with CheckpointedFunction {
  var opHighTempCnt: Long = 0
  var keyedCntState: ValueState[Long] = _
  var opCntState: ListState[Long] = _

  override def flatMap(t: SensorReading, collector: Collector[(String, Long, Long)]): Unit = {
    if (t.temperature > threshold) {
      opHighTempCnt += 1
      val keyHighTempCnt = keyedCntState.value() + 1
      keyedCntState.update(keyHighTempCnt)
      collector.collect((t.id, keyHighTempCnt, opHighTempCnt))
    }
  }
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val keyCntDescriptor = new ValueStateDescriptor[Long]("keyedCnt", classOf[Long])
    keyedCntState = context.getKeyedStateStore.getState(keyCntDescriptor)
    val opCntDescriptor = new ListStateDescriptor[Long]("opCnt", classOf[Long])
    opCntState = context.getOperatorStateStore.getListState(opCntDescriptor)

    opHighTempCnt = opCntState.get().asScala.sum
  }
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    opCntState.clear()
    opCntState.add(opHighTempCnt)
  }
}