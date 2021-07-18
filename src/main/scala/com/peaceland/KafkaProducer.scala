package com.peaceland

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.SQLContext
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}

object KafkaProducerApp extends App {

  val props:Properties = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = "report"

  try {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.csv("data/drone.csv")


    df.rdd.foreachPartition((partitions: Iterator[Row]) => {
      partitions.foreach((row: Row) => {
          val record = new ProducerRecord[String, String](topic, row(0).toString, row(0).toString + "," + row(1).toString + "," + row(2).toString + "," + row(3).toString + "," +  row(4).toString + "," +  row(5).toString)
          val metadata = producer.send(record)

          printf(s"sent record(key=%s value=%s) " +
            "meta(partition=%d, offset=%d)\n",
            record.key(), record.value(),
            metadata.get().partition(),
            metadata.get().offset())
      })
    })
  } catch{
    case e:Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
