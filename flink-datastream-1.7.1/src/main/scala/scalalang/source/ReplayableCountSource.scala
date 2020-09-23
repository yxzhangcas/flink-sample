package scalalang.source

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

class ReplayableCountSource extends SourceFunction[Long] with CheckpointedFunction {
  var isRunning = true
  var cnt: Long = _
  var offsetState: ListState[Long] = _

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning && cnt < Long.MaxValue) {
      /*
      使用锁对象对偏移推进和数据发送进行同步处理
       */
      ctx.getCheckpointLock.synchronized{
        cnt += 1
        ctx.collect(cnt)
      }
      Thread.sleep(500)
    }
  }
  override def cancel(): Unit = {
    isRunning = false
  }
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val desc = new ListStateDescriptor[Long]("offset", classOf[Long])
    offsetState = context.getOperatorStateStore.getListState(desc)
    /*
    疑问：这里的get获取的迭代器指向的是List的第一个成员还是最后一个？
    解答：没有关系，因为snapshotState方法保证List里面最多只有一个成员
     */
    val iter = offsetState.get()
    cnt = if (null == iter || !iter.iterator().hasNext) {
      -1L
    } else {
      iter.iterator().next()
    }
  }
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    /*
    状态原语存储里面只保存一个数据
     */
    offsetState.clear()
    offsetState.add(cnt)
  }
}
