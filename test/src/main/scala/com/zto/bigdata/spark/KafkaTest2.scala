package com.zto.bigdata.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ChengLong on 2017-10-08.
  */
object KafkaTest2 {

  val map = Map[String, Object](
    "bootstrap.servers" -> "10.9.15.37:9092,10.9.15.38:9092",
    "group.id" -> "ScalaTest",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    // 如果有记录offset，则从上次的位置继续消费。如果没有记录，则从起始位置消费
    "auto.offset.reset" -> "earliest",
    // 关闭自动维护offset
    "enable.auto.commit" -> (false: java.lang.Boolean))

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ScalaTest")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))
    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set("zto_scan_spark"), map))
    // 主动维护kafka offset
    dstream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}