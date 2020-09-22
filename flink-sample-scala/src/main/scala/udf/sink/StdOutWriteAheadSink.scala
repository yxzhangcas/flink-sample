package udf.sink

import java.lang
import java.util.UUID

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink
import scala.collection.JavaConverters._
import udf.checkpoint.FileCheckpointCommitter

class StdOutWriteAheadSink extends GenericWriteAheadSink[(String, Double)](
  new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
  //隐式转换
  createTypeInformation[(String, Double)].createSerializer(new ExecutionConfig),
  UUID.randomUUID().toString
) {
  override def sendValues(values: lang.Iterable[(String, Double)], checkpointId: Long, timestamp: Long): Boolean = {
    for (r <- values.asScala) {
      println(r)
    }
    true
  }
}
