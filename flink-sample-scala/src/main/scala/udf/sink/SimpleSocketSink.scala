package udf.sink

import java.io.PrintStream
import java.net.{InetAddress, Socket}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import util.SensorReading

class SimpleSocketSink(val host: String, val port: Int) extends RichSinkFunction[SensorReading] {
  //Java的net.Socket类和io.PrintStream类
  var socket: Socket = _
  var writer: PrintStream = _

  override def open(parameters: Configuration): Unit = {
    /*
    Java语言的IO和网络编程还是得熟悉一下
     */
    socket = new Socket(InetAddress.getByName(host), port)
    writer = new PrintStream(socket.getOutputStream)
  }
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    writer.println(value.toString)
    writer.flush()
  }
  override def close(): Unit = {
    writer.close()
    socket.close()
  }
}
