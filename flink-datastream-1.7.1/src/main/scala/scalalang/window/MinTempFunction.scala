package scalalang.window

import org.apache.flink.api.common.functions.ReduceFunction

class MinTempFunction extends ReduceFunction[(String, Double)] {
  override def reduce(t: (String, Double), t1: (String, Double)): (String, Double) = {
    (t._1, t._2.min(t1._2))
  }
}
