package com.peaceland.datalake

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object ParquetTest {
  val path = "hdfs://localhost:9000/user/hadoop/"

  def main(args: Array[String]) = {
    // Two threads local[2]
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ParquetTest")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    writeParquet(sc, sqlContext)
    readParquet(sqlContext)
  }

  def writeParquet(sc: SparkContext, sqlContext: SQLContext) = {
    // Read file as RDD
    val rdd = sqlContext.read.format("csv").option("header", "true").load(path + "drone.csv")
    // Convert rdd to data frame using toDF; the following import is required to use toDF function.
    val df: DataFrame = rdd.toDF()
    // Write file to parquet
    df.write.parquet(path + "drone.parquet");
  }

  def readParquet(sqlContext: SQLContext): DataFrame = {
    // read back parquet to DF
    sqlContext.read.parquet(path + "drone.parquet")
  }
}
