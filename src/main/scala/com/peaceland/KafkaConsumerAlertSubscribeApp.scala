package com.peaceland

import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
object KafkaConsumerAlertSubscribeApp extends App {

  val props:Properties = new Properties()
  props.put("group.id", "consumer-group")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  //props.put("enable.auto.commit", "true")
  props.put("auto.offset.reset", "latest")
  //props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("Alert")
  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(1000)

      records.forEach(record => {
        println("Topic: " + record.topic() +
          ",Key: " + record.key() +
          ",Value: " + record.value() +
          ", Offset: " + record.offset() +
          ", Partition: " + record.partition())
      })
    }
  } catch {
    case e:Exception => e.printStackTrace()
  } finally {
    consumer.close()
  }
}