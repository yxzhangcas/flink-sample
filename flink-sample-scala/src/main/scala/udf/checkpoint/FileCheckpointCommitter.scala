package udf.checkpoint

import java.nio.file.{Files, Paths}

import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter

class FileCheckpointCommitter(val basePath: String) extends CheckpointCommitter {
  private var tempPath: String = _

  override def commitCheckpoint(subtaskIdx: Int, checkpointID: Long): Unit = {
    val commitPath = Paths.get(tempPath + "/" + subtaskIdx)
    val hexID = "0x" + StringUtils.leftPad(checkpointID.toHexString, 16, "0")
    Files.write(commitPath, hexID.getBytes)
  }
  override def isCheckpointCommitted(subtaskIdx: Int, checkpointID: Long): Boolean = {
    val commitPath = Paths.get(tempPath + "/" + subtaskIdx)
    if (!Files.exists(commitPath)) {
      false
    } else {
      val hexID = Files.readAllLines(commitPath).get(0)
      val checkpointed = java.lang.Long.decode(hexID)
      //检查点ID要满足递增规律才能这样进行判断
      checkpointID <= checkpointed
    }
  }
  override def createResource(): Unit = {
    this.tempPath = basePath + "/" + this.jobId
    Files.createDirectory(Paths.get(tempPath))
  }
  override def open(): Unit = {
    //不用建立连接
  }
  override def close(): Unit = {
    //不用关闭连接
  }
}
