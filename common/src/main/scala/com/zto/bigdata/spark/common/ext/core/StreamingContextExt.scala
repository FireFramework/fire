package com.zto.bigdata.spark.common.ext.core

import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy, RocketMqUtils}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils

import scala.collection.JavaConversions

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
  def createDirectStream(kafkaParams: Map[String, Object] = this.kafkaParams(), topics: Set[String] = SparkUtils.topicSplit(GlobalConstants.KafkaConf.kafkaTopics()), level: StorageLevel = StorageLevel.NONE): DStream[ConsumerRecord[String, String]] = {
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
  def createDirectStream2(kafkaParams: Map[String, Object] = this.kafkaParams(GlobalConstants.KafkaConf.kafkaGroupId("2"), GlobalConstants.KafkaConf.kafkaBrokers("2")), topics: Set[String] = SparkUtils.topicSplit(GlobalConstants.KafkaConf.kafkaTopics("2")), level: StorageLevel = StorageLevel.NONE): DStream[ConsumerRecord[String, String]] = {
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
  def createDirectStream3(kafkaParams: Map[String, Object] = this.kafkaParams(GlobalConstants.KafkaConf.kafkaGroupId("3"), GlobalConstants.KafkaConf.kafkaBrokers("3")), topics: Set[String] = SparkUtils.topicSplit(GlobalConstants.KafkaConf.kafkaTopics("3")), level: StorageLevel = StorageLevel.NONE): DStream[ConsumerRecord[String, String]] = {
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
  def kafkaParams(groupId: String = GlobalConstants.KafkaConf.kafkaGroupId(), kafkaBrokers: String = GlobalConstants.KafkaConf.kafkaBrokers(), offset: String = GlobalConstants.KafkaConf.kafkaStartingOffset, commit: Boolean = GlobalConstants.KafkaConf.kafkaEnableAutoCommit): Map[String, Object] = {
    // 如果配置文件中没有指定spark.kafka.group.id，则默认为appName
    val kafkaGroupId = if (StringUtils.isNotBlank(groupId)) groupId else ssc.sparkContext.appName
    SparkUtils.kafkaParams(kafkaGroupId, kafkaBrokers, offset)
  }

  /**
    * 构建RocketMQ拉取消息的DStream流
    *
    * @param rocketParam
    * rocketMQ相关消费参数
    * @param groupId
    * groupId
    * @param topics
    * topic列表
    * @param consumerStrategy
    * 从何处开始消费
    * @param autoCommit
    * 是否自动提交
    * @return
    * rocketMQ DStream
    */
  def createRocketPullStream(rocketParam: java.util.Map[String, String] = this.rocketParams(), groupId: String = GlobalConstants.RocketConf.rocketGroupId(), topics: String = GlobalConstants.RocketConf.rocketTopics(), consumerStrategy: ConsumerStrategy = GlobalConstants.RocketConf.rocketStartingOffset(), autoCommit: Boolean = GlobalConstants.RocketConf.rocketEnableAutoCommit): InputDStream[MessageExt] = {
    RocketMqUtils.createMQPullStream(this.ssc, groupId, JavaConversions.asJavaCollection(topics.split(",").toList),
      consumerStrategy,
      autoCommit, forceSpecial = false, failOnDataLoss = false,
      LocationStrategy.PreferConsistent, rocketParam)
  }

  /**
    * 构建RocketMQ拉取消息的DStream流
    *
    * @param rocketParam
    * rocketMQ相关消费参数
    * @param groupId
    * groupId
    * @param topics
    * topic列表
    * @param consumerStrategy
    * 从何处开始消费
    * @param autoCommit
    * 是否自动提交
    * @return
    * rocketMQ DStream
    */
  def createRocketPullStream2(rocketParam: java.util.Map[String, String] = this.rocketParams(GlobalConstants.RocketConf.rocketGroupId("2"), GlobalConstants.RocketConf.rocketNameServer("2"), GlobalConstants.RocketConf.rocketConsumerTag("2")), groupId: String = GlobalConstants.RocketConf.rocketGroupId("2"), topics: String = GlobalConstants.RocketConf.rocketTopics("2"), consumerStrategy: ConsumerStrategy = GlobalConstants.RocketConf.rocketStartingOffset("2"), autoCommit: Boolean = GlobalConstants.RocketConf.rocketEnableAutoCommit): InputDStream[MessageExt] = {
    RocketMqUtils.createMQPullStream(this.ssc, groupId, JavaConversions.asJavaCollection(topics.split(",").toList),
      consumerStrategy,
      autoCommit, forceSpecial = false, failOnDataLoss = false,
      LocationStrategy.PreferConsistent, rocketParam)
  }

  /**
    * 构建RocketMQ拉取消息的DStream流
    *
    * @param rocketParam
    * rocketMQ相关消费参数
    * @param groupId
    * groupId
    * @param topics
    * topic列表
    * @param consumerStrategy
    * 从何处开始消费
    * @param autoCommit
    * 是否自动提交
    * @return
    * rocketMQ DStream
    */
  def createRocketPullStream3(rocketParam: java.util.Map[String, String] = this.rocketParams(GlobalConstants.RocketConf.rocketGroupId("3"), GlobalConstants.RocketConf.rocketNameServer("3"), GlobalConstants.RocketConf.rocketConsumerTag("3")), groupId: String = GlobalConstants.RocketConf.rocketGroupId("3"), topics: String = GlobalConstants.RocketConf.rocketTopics("3"), consumerStrategy: ConsumerStrategy = GlobalConstants.RocketConf.rocketStartingOffset("3"), autoCommit: Boolean = GlobalConstants.RocketConf.rocketEnableAutoCommit): InputDStream[MessageExt] = {
    RocketMqUtils.createMQPullStream(this.ssc, groupId, JavaConversions.asJavaCollection(topics.split(",").toList),
      consumerStrategy,
      autoCommit, forceSpecial = false, failOnDataLoss = false,
      LocationStrategy.PreferConsistent, rocketParam)
  }

  /**
    * rocket配置信息
    *
    * @param groupId
    * 消费组
    * @return
    * kafka相关配置
    */
  def rocketParams(groupId: String = GlobalConstants.RocketConf.rocketGroupId(), rocketNameServer: String = GlobalConstants.RocketConf.rocketNameServer(), tag: String = GlobalConstants.RocketConf.rocketConsumerTag()): java.util.Map[String, String] = {
    // 如果配置文件中没有指定spark.rocket.group.id，则默认为appName
    val rocketGroupId = if (StringUtils.isNotBlank(groupId)) groupId else ssc.sparkContext.appName
    SparkUtils.rocketParams(rocketGroupId, rocketNameServer, tag)
  }

  /**
    * 开启streaming
    */
  def startAwaitTermination(): Unit = {
    ssc.start()
    ssc.awaitTermination()
  }
}