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
  val topic = "Report"

  try {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    //val df = spark.read.csv("data/drone.csv")

    val rddFromFile = spark.sparkContext.textFile("data/drone.csv")

    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })

    rdd.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }.foreachPartition((partitions: Iterator[Array[String]]) => {
      partitions.foreach((row: Array[String]) => {
          val record = new ProducerRecord[String, String](topic, row(0), row(0) + "," + row(1) + "," + row(2) + "," + row(3) + "," +  row(4) + "," +  row(5))
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
