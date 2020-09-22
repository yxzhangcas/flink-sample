package udf.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

/*
SourceFunction定义的是非并行数据源连接器，只能单任务运行
 */
class CountSource extends SourceFunction[Long] {
  var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    var cnt: Long = -1
    while (isRunning && cnt < Long.MaxValue) {
      cnt += 1
      ctx.collect(cnt)
      Thread.sleep(500)
    }
  }
  override def cancel(): Unit = {
    isRunning = false
  }
}
