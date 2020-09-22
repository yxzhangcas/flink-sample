package udf.sink

import java.io.BufferedWriter
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala._

class TransactionalFileSink(val targetPath: String, val tempPath: String)
  extends TwoPhaseCommitSinkFunction[(String, Double), String, Void](
    //隐式转换
    createTypeInformation[String].createSerializer(new ExecutionConfig),
    createTypeInformation[Void].createSerializer(new ExecutionConfig)
  ) {
  var transactionWriter: BufferedWriter = _

  override def beginTransaction(): String = {
    val timeNow = LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    val transactionFile = s"$timeNow-$taskIdx"
    val tFilePath = Paths.get(s"$tempPath/$transactionFile")
    Files.createFile(tFilePath)
    this.transactionWriter = Files.newBufferedWriter(tFilePath)
    println(s"Creating Transaction File: $tFilePath")
    transactionFile
  }
  override def invoke(transaction: String, value: (String, Double), context: SinkFunction.Context[_]): Unit = {
    transactionWriter.write(value.toString())
    transactionWriter.write('\n')
  }
  override def preCommit(transaction: String): Unit = {
    transactionWriter.flush()
    transactionWriter.close()
  }
  override def commit(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    if (Files.exists(tFilePath)) {
      val cFilePath = Paths.get(s"$targetPath/$transaction")
      Files.move(tFilePath, cFilePath)
    }
  }
  override def abort(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    if (Files.exists(tFilePath)) {
      Files.delete(tFilePath)
    }
  }
}
