package com.peaceland.datalake

import com.peaceland.datalake.ParquetTest.{readParquet, writeParquet}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{min, max}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


object AnalyseData {

  def showData(data : DataFrame) = {
    data.show()
  }

  def numberOfPeopleRiot(data : DataFrame, seuil : Int) = {
    data.filter(data.col("peace_score") < seuil).count()
  }

  def theUnpeacefulMan(data : DataFrame) ={
    data.select(data.col("citizen_name"), data.col("peace_score"))
      .orderBy(data.col("peace_score")).first()
  }

  def theZenMan(data : DataFrame) ={

    data.select(data.col("citizen_name"), data.col("peace_score"))
      .sort(data.col("peace_score").desc).first()
  }


  def getStats(data : DataFrame) = {
    println("Number of people : " + numberOfPeopleRiot(data, 101))
    println("Number of people unpeaceful : " + numberOfPeopleRiot(data, 40))
    println("Number of people g̶o̶u̶l̶a̶g̶e̶d̶ put in peace camp : " + numberOfPeopleRiot(data, 10))
    println("The most peaceful man : " + theZenMan(data))
    println("The most unpeaceful man : " + theUnpeacefulMan(data))
  }

  def main(args: Array[String]) = {
    // Two threads local[2]
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("ParquetTest")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    val data = readParquet(sqlContext)

    showData(data)
    getStats(data)
  }
}
