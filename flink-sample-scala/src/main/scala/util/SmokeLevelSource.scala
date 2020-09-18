package util

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import util.SmokeLevel.SmokeLevel

import scala.util.Random

class SmokeLevelSource extends RichParallelSourceFunction[SmokeLevel] {
  var running = true
  override def run(sourceContext: SourceFunction.SourceContext[SmokeLevel]): Unit = {
    val rand = new Random()
    while (running) {
      if (rand.nextGaussian() > 0.8) {
        sourceContext.collect(SmokeLevel.High)
      } else {
        sourceContext.collect(SmokeLevel.Low)
      }
      Thread.sleep(1000)
    }
  }
  override def cancel(): Unit = {
    running = false
  }
}
