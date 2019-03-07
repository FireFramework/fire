package com.zto.bigdata.spark

import org.apache.spark.sql.SparkSession

object SparkSessionTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSessionTest")
      .master("local[*]")
      .getOrCreate()
    val dataSet = spark.range(1, 10)
    dataSet.printSchema()
    println(dataSet.count())
    dataSet.createOrReplaceTempView("test")
    println("========================================")
    spark.sql("select * from test").show()
    Thread.sleep(1000000)
  }
}
