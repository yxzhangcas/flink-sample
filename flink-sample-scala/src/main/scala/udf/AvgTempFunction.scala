package udf

import org.apache.flink.api.common.functions.AggregateFunction

class AvgTempFunction extends AggregateFunction[(String, Double), (String, Double, Int), (String, Double)] {
  override def createAccumulator(): (String, Double, Int) = {
    ("", 0.0, 0)
  }

  override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = {
    (in._1, in._2 + acc._2, acc._3 + 1)
  }

  override def getResult(acc: (String, Double, Int)): (String, Double) = {
    (acc._1, acc._2 / acc._3)
  }

  override def merge(acc: (String, Double, Int), acc1: (String, Double, Int)): (String, Double, Int) = {
    (acc._1, acc._2 + acc1._2, acc._3 + acc1._3)
  }
}
