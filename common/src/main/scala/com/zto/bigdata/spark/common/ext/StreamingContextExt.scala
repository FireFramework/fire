package com.zto.bigdata.spark.common.ext

import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils

/**
  * StreamingContext扩展
  *
  * @param ssc
  * StreamingContext对象
  * @author ChengLong 2019-5-18 11:03:59
  */
class StreamingContextExt(ssc: StreamingContext) {

  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

  /**
    * 创建DStream流
    *
    * @param kafkaParams
    * kafka参数
    * @param topics
    * topic列表
    * @return
    * DStream
    */
  def createDirectStream(kafkaParams: Map[String, Object] = this.kafkaParams(), topics: Set[String] = SparkUtils.topicSplit(GlobalConstants.SparkConf.kafkaTopics()), level: StorageLevel = StorageLevel.NONE): DStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
  }

  /**
    * 创建DStream流，匹配后缀为2的配置
    *
    * @param kafkaParams
    * kafka参数
    * @param topics
    * topic列表
    * @return
    * DStream
    */
  def createDirectStream2(kafkaParams: Map[String, Object] = this.kafkaParams(GlobalConstants.SparkConf.kafkaGroupId("2"), GlobalConstants.SparkConf.kafkaBrokers("2")), topics: Set[String] = SparkUtils.topicSplit(GlobalConstants.SparkConf.kafkaTopics("2")), level: StorageLevel = StorageLevel.NONE): DStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
  }

  /**
    * 创建DStream流，匹配后缀为3的配置
    *
    * @param kafkaParams
    * kafka参数
    * @param topics
    * topic列表
    * @return
    * DStream
    */
  def createDirectStream3(kafkaParams: Map[String, Object] = this.kafkaParams(GlobalConstants.SparkConf.kafkaGroupId("3"), GlobalConstants.SparkConf.kafkaBrokers("3")), topics: Set[String] = SparkUtils.topicSplit(GlobalConstants.SparkConf.kafkaTopics("3")), level: StorageLevel = StorageLevel.NONE): DStream[ConsumerRecord[String, String]] = {
    KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
  }

  /**
    * kafka配置信息
    *
    * @param groupId
    * 消费组
    * @param offset
    * offset位点，smallest、largest，默认为largest
    * @return
    * kafka相关配置
    */
  def kafkaParams(groupId: String = GlobalConstants.SparkConf.kafkaGroupId(), kafkaBrokers: String = GlobalConstants.SparkConf.kafkaBrokers(), offset: String = GlobalConstants.SparkConf.kafkaStartingOffset, commit: Boolean = GlobalConstants.SparkConf.kafkaEnableAutoCommit): Map[String, Object] = {
    // 如果配置文件中没有指定spark.kafka.group.id，则默认为appName
    val kafkaGroupId = if (StringUtils.isNotBlank(groupId)) groupId else ssc.sparkContext.appName
    SparkUtils.kafkaParams(kafkaGroupId, kafkaBrokers, offset)
  }

  /**
    * 开启streaming
    */
  def startAwaitTermination(): Unit = {
    ssc.start()
    ssc.awaitTermination()
  }
}