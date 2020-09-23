package scalalang.function

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

class SplitIdFlatMap extends FlatMapFunction[String, String] {
  override def flatMap(t: String, collector: Collector[String]): Unit = t.split("_").foreach(t => collector.collect(t))
}
