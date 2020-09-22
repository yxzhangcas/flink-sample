package udf.source

import java.time.Instant
import java.util.Calendar

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import util.SensorReading

import scala.collection.JavaConverters._
import scala.util.Random

class ResettableSensorSource extends RichParallelSourceFunction[SensorReading] with CheckpointedFunction {
  var running = true
  var readings: Array[SensorReading] = _
  var sensorsState: ListState[SensorReading] = _

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    while (running) {
      ctx.getCheckpointLock.synchronized{
        for (i <- readings.indices) {
          /*
          读取旧状态，生成新状态，发送新状态，保留新状态【可回溯】
           */
          val reading = readings(i)
          val newTime = reading.timestamp + 100
          rand.setSeed(newTime ^ reading.temperature.toLong)
          val newTemp = reading.temperature + (rand.nextGaussian() * 0.5)
          val newReading = SensorReading(reading.id, newTime, newTemp)
          readings(i) = newReading
          ctx.collect(newReading)
        }
      }
      Thread.sleep(100)
    }
  }
  override def cancel(): Unit = {
    running = false
  }
  override def initializeState(context: FunctionInitializationContext): Unit = {
    //联合列表状态
    this.sensorsState = context.getOperatorStateStore.getUnionListState(
      new ListStateDescriptor[SensorReading]("sensorState", classOf[SensorReading])
    )
    val sensorStateIter = sensorsState.get().iterator()
    if (!sensorStateIter.hasNext) {
      val rand = new Random()
      val numTasks = getRuntimeContext.getNumberOfParallelSubtasks
      val thisTask = getRuntimeContext.getIndexOfThisSubtask
      //Calendar已经过时，改用Instant
      //val curTime = Calendar.getInstance().getTimeInMillis
      val curTime = Instant.now().toEpochMilli
      this.readings = (0 until 10)
        .map(i => {
          val idx = thisTask + i * numTasks
          val sensorId = s"sensor_$idx"
          val temp = 65 + rand.nextGaussian() * 20
          SensorReading(sensorId, curTime, temp)
        })
        .toArray
    } else {
      val numTasks = getRuntimeContext.getNumberOfParallelSubtasks
      val thisTask = getRuntimeContext.getIndexOfThisSubtask
      val allReadings = sensorStateIter.asScala.toSeq
      this.readings = allReadings
        .zipWithIndex
        .filter(x => x._2 % numTasks == thisTask)
        .map(_._1)
        .toArray
    }
  }
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    this.sensorsState.update(readings.toList.asJava)
  }
}
