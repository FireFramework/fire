package com.zto.bigdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * 结构化流
  */
object StructedStreamingTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StructedStreamingTest").getOrCreate()
    val lines = spark.readStream.format("socket").option("host", "10.9.15.37:9092,10.9.15.38:9092").option("port", "9999").load()
    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCount = words.groupBy("value").count()
    wordCount.printSchema()
    println("count--> " + wordCount.count())
    wordCount.show()
    val query = wordCount.writeStream.outputMode(OutputMode.Complete()).format("console").start()

    query.awaitTermination()
  }
}
