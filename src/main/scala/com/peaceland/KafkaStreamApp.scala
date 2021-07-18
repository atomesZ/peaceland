package com.peaceland

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object KafkaStreamApp extends App {

  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-scala-example")
    val bootstrapServers = "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  val builder = new StreamsBuilder

  val reportKStream: KStream[String, String] = builder.stream[String, String]("Report")

  val reportConsumer: KStream[String, String] = reportKStream
  reportConsumer.to("ReportConsumer")

  def report2Alert(V: String): String =
  {
    val Vlist = V.split(",")
    Vlist(1) + "," + Vlist(2) + "," + Vlist(3)
  }

  val peacescoreAlertThreshold = 40

  val alertConsumer: KStream[String, String] = reportKStream.filter((K, V) => V.split(",")(4).toInt <= peacescoreAlertThreshold)
                                                            .map((K, V) => (K, report2Alert(V)))
  alertConsumer.to("Alert")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}