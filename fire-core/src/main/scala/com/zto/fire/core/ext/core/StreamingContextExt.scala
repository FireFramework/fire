package com.zto.fire.core.ext.core

import com.zto.fire.common.util.{GlobalConstants, ValueUtils}
import com.zto.fire.core.util.SparkUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy, RocketMqUtils}
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
  def createDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, keyNum: Int = 1): DStream[ConsumerRecord[String, String]] = {
    val finalKafkaTopic = if (topics == null) SparkUtils.topicSplit(GlobalConstants.KafkaConf.kafkaTopics(keyNum)) else topics
    ValueUtils.requireNonNull(finalKafkaTopic, s"kafka topic不能为空，请在配置文件中指定：spark.kafka.topics$keyNum")
    val finalKafkaParams = if (kafkaParams == null) this.kafkaParams(keyNum = keyNum) else kafkaParams

    KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](finalKafkaTopic, finalKafkaParams))
  }

  /**
    * kafka配置信息
    *
    * @param groupId
    * 消费组
    * @param offset
    * offset位点，smallest、largest，默认为largest
    * @param keyNum
    * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
    * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
    * @return
    * kafka相关配置
    */
  def kafkaParams(groupId: String = null, kafkaBrokers: String = null, offset: String = null, autoCommit: Boolean = false, keyNum: Int = 1): Map[String, Object] = {
    // 如果配置文件中没有指定spark.kafka.group.id，则默认为appName
    val finalKafkaGroupId = if (StringUtils.isBlank(groupId)) {
      if (StringUtils.isNotBlank(GlobalConstants.KafkaConf.kafkaGroupId(keyNum))) {
        GlobalConstants.KafkaConf.kafkaGroupId(keyNum)
      } else {
        ssc.sparkContext.appName
      }
    } else {
      groupId
    }

    com.zto.fire.common.util.KafkaUtils.kafkaParams(finalKafkaGroupId, kafkaBrokers, offset, autoCommit, keyNum)
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
  def createRocketPullStream(rocketParam: java.util.Map[String, String] = null, groupId: String = null, topics: String = null, consumerStrategy: ConsumerStrategy = null, autoCommit: Boolean = false, keyNum: Int = 1): InputDStream[MessageExt] = {
    val finalGroupId = if (StringUtils.isBlank(groupId)) GlobalConstants.RocketConf.rocketGroupId(keyNum) else groupId
    val finalRocketParam = if (rocketParam == null || rocketParam.size() == 0) this.rocketParams(finalGroupId, keyNum = keyNum) else rocketParam
    val finalTopics = if (StringUtils.isBlank(topics)) GlobalConstants.RocketConf.rocketTopics(keyNum) else topics
    val finalConsumerStrategy = if (consumerStrategy == null) GlobalConstants.RocketConf.rocketStartingOffset(keyNum) else consumerStrategy
    val finalAutoCommit = if (autoCommit == null) GlobalConstants.RocketConf.rocketEnableAutoCommit(keyNum) else autoCommit

    RocketMqUtils.createMQPullStream(this.ssc, finalGroupId, JavaConversions.asJavaCollection(finalTopics.split(",").toList),
      finalConsumerStrategy,
      finalAutoCommit, forceSpecial = false, failOnDataLoss = false,
      LocationStrategy.PreferConsistent, finalRocketParam)
  }

  /**
    * rocket配置信息
    *
    * @param groupId
    * 消费组
    * @return
    * kafka相关配置
    */
  def rocketParams(groupId: String = null, rocketNameServer: String = null, tag: String = null, keyNum: Int = 1): java.util.Map[String, String] = {
    // 如果配置文件中没有指定spark.rocket.group.id，则默认为appName
    val rocketGroupId = if (StringUtils.isNotBlank(groupId)) groupId else ssc.sparkContext.appName
    SparkUtils.rocketParams(rocketGroupId, rocketNameServer, tag, keyNum)
  }

  /**
    * 开启streaming
    */
  def startAwaitTermination(): Unit = {
    ssc.start()
    ssc.awaitTermination()
    Thread.currentThread().join()
  }
}