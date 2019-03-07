package com.zto.bigdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object KafkaTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaTest").getOrCreate()
    val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.9.15.37:9092,10.9.15.38:9092").option("subscribe", "zto_scan_spark").load()
    import spark.implicits._
    kafkaDF.createOrReplaceTempView("test")
    val dstream = spark.sql("select count(1) from test").writeStream.outputMode(OutputMode.Complete()).format("console").start()
    dstream.awaitTermination()
  }
}
