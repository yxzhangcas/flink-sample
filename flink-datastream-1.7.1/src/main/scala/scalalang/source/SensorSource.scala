package scalalang.source

import java.time.Instant

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import scalalang.util.SensorReading

import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading] {
  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    var curFTemp = (1 to 10).map(
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20)))
    while (running) {
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
      //Calendar已经过时，改用Instant
      //val curTime = Calendar.getInstance().getTimeInMillis
      val curTime = Instant.now.toEpochMilli
      curFTemp.foreach(t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
