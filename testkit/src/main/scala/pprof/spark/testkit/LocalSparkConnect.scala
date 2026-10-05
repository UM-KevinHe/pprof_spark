package pprof.spark.testkit

import java.net.ServerSocket

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectService

/** A Spark Connect client session served by a Connect server inside the test JVM (layer T8).
  *
  * The server runs on the SparkContext of [[LocalSpark.session]]; the client is the Scala Spark
  * Connect client, so every operation, including typed closures, goes through the Connect
  * protocol. The test JVM also holds Classic Spark, so this topology cannot detect client-side
  * use of Classic-only classes; ADR-0002 records that limit and the classpath order it needs.
  */
object LocalSparkConnect {

  lazy val session: SparkSession = {
    val port = freePort()
    // The server reads its port from the SparkContext configuration, which takes spark.* system
    // properties when the local Classic session is created.
    System.setProperty("spark.connect.grpc.binding.port", port.toString)
    SparkConnectService.start(LocalSpark.session.sparkContext)
    org.apache.spark.sql.connect.SparkSession
      .builder()
      .remote(s"sc://localhost:$port")
      .getOrCreate()
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }
}
