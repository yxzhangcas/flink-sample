package scalalang.function

import org.apache.flink.api.common.functions.MapFunction

/*
不对原数据流进行任何改变，只用于在预设的数据量触发异常，验证一致性输出
但并没有使用状态变量和检查点技术对cnt进行保存，也就是说只验证原消息数据，不包括此处的验证状态
程序恢复后，cnt重新初始化，故可以实现周期性触发异常
 */
class FailingMapper[IN](val failInterval: Int) extends MapFunction[IN, IN] {
  var cnt: Int = 0
  override def map(t: IN): IN = {
    cnt += 1
    if (cnt > failInterval) {
      throw new RuntimeException("Fail application to demonstrate output consistency")
    }
    t
  }
}