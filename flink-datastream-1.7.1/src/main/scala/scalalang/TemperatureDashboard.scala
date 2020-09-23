package scalalang

import java.util.concurrent.CompletableFuture

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala._
import org.apache.flink.queryablestate.client.QueryableStateClient

object TemperatureDashboard {
  val proxyHost = "127.0.0.1"
  val proxyPort = 9069
  val jobId = "e8bdc34aea6e31e60a8a337a8f4221de"  //这里的JobID需要根据实际运行的服务来确定，应该也有接口可以获取
  val numSensors = 5
  val refreshInterval = 1000

  def main(args: Array[String]): Unit = {
    val client = new QueryableStateClient(proxyHost, proxyPort)
    val futures = new Array[CompletableFuture[ValueState[(String, Double)]]](numSensors)
    val results = new Array[Double](numSensors)

    val header = (for (i <- 0 until numSensors) yield "sensor_" + (i + 1)).mkString("\t| ")
    println(header)

    while (true) {
      for (i <- 0 until numSensors) {
        futures(i) = queryState("sensor_" + (i + 1), client)
      }
      for (i <- 0 until numSensors) {
        results(i) = futures(i).get().value()._2
      }
      val line = results.map(t => f"$t%1.3f").mkString("\t|")
      println(line)

      Thread.sleep(refreshInterval)
    }
    client.shutdownAndWait()
  }
  def queryState(key: String, client: QueryableStateClient): CompletableFuture[ValueState[(String, Double)]] = {
    client.getKvState[String, ValueState[(String, Double)], (String, Double)](
      JobID.fromHexString(jobId), "maxTemperature", key, Types.STRING,
      new ValueStateDescriptor[(String, Double)]("", Types.TUPLE[(String, Double)]))
  }
}
