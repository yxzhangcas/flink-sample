package scalalang.function

import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.Executors

import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import scalalang.util.SensorReading

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DerbyAsyncFunction extends AsyncFunction[SensorReading, (String, String)] {
  private lazy val cachingPoolExecCtx = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  private lazy val directExecCtx = ExecutionContext.fromExecutor(org.apache.flink.runtime.concurrent.Executors.directExecutor)

  override def asyncInvoke(input: SensorReading, resultFuture: ResultFuture[(String, String)]): Unit = {
    val sensor = input.id
    val room = Future {
      val conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
      val query = conn.createStatement()
      val result = query.executeQuery(s"SELECT room FROM SensorLocations WHERE sensor = '$sensor'")
      val room = if (result.next()) {
        result.getString(1)
      } else {
        "UNKNOWN ROOM"
      }
      result.close()
      query.close()
      conn.close()
      Thread.sleep(2000L)
      room
    }(cachingPoolExecCtx)
    room.onComplete{
      case Success(value) => resultFuture.complete(Seq((sensor, value)))
      case Failure(exception) => resultFuture.completeExceptionally(exception)
    }(directExecCtx)
  }
}
