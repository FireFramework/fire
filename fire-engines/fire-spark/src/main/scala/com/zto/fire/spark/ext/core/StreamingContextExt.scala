package com.zto.fire.spark.ext.core

import com.zto.fire.common.conf.{FireKafkaConf, FireRocketMQConf}
import com.zto.fire.spark.util.{RocketMQUtils, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy, RocketMQConfig, RocketMqUtils}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions

/**
 * StreamingContext扩展
 *
 * @param ssc
 * StreamingContext对象
 * @author ChengLong 2019-5-18 11:03:59
 */
private[fire] class StreamingContextExt(ssc: StreamingContext) {

  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

  private lazy val logger = LoggerFactory.getLogger(this.getClass)
  private[this] lazy val appName = ssc.sparkContext.appName

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
  def createDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, groupId: String = null, keyNum: Int = 1): DStream[ConsumerRecord[String, String]] = {
    // kafka topic优先级：配置文件 > topics参数
    val confTopic = FireKafkaConf.kafkaTopics(keyNum)
    val finalKafkaTopic = if (StringUtils.isNotBlank(confTopic)) SparkUtils.topicSplit(confTopic) else topics
    require(finalKafkaTopic != null && finalKafkaTopic.nonEmpty, s"kafka topic不能为空，请在配置文件中指定：spark.kafka.topics$keyNum")
    this.logger.info(s"kafka topic is $finalKafkaTopic")

    val confKafkaParams = com.zto.fire.common.util.KafkaUtils.kafkaParams(kafkaParams, groupId, keyNum = keyNum)
    require(confKafkaParams.nonEmpty, "kafka相关配置不能为空！")
    require(confKafkaParams.contains("bootstrap.servers"), s"kafka bootstrap.servers不能为空，请在配置文件中指定：spark.kafka.brokers.name$keyNum")
    require(confKafkaParams.contains("group.id"), s"kafka group.id不能为空，请在配置文件中指定：spark.kafka.group.id$keyNum")

    KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](finalKafkaTopic, confKafkaParams))
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
   * @return
   * rocketMQ DStream
   */
  def createRocketPullStream(rocketParam: java.util.Map[String, String] = null,
                             groupId: String = this.appName,
                             topics: String = null,
                             tag: String = null,
                             consumerStrategy: ConsumerStrategy = ConsumerStrategy.lastest,
                             locationStrategy: LocationStrategy = LocationStrategy.PreferConsistent,
                             keyNum: Int = 1): InputDStream[MessageExt] = {

    // 获取topic信息，配置文件优先级高于代码中指定的
    val confTopics = FireRocketMQConf.rocketTopics(keyNum)
    val finalTopics = if (StringUtils.isNotBlank(confTopics)) confTopics else topics
    require(StringUtils.isNotBlank(finalTopics), s"RocketMQ的Topics不能为空，请在配置文件中指定：spark.rocket.topics$keyNum")

    // 起始消费位点
    val confOffset = FireRocketMQConf.rocketStartingOffset(keyNum)
    val finalConsumerStrategy = if (StringUtils.isNotBlank(confOffset)) RocketMQUtils.valueOfStrategy(confOffset) else consumerStrategy

    // 是否自动提交offset
    val finalAutoCommit = FireRocketMQConf.rocketEnableAutoCommit(keyNum)

    // groupId信息
    val confGroupId = FireRocketMQConf.rocketGroupId(keyNum)
    val finalGroupId = if (StringUtils.isNotBlank(confGroupId)) confGroupId else groupId
    require(StringUtils.isNotBlank(finalGroupId), s"RocketMQ的groupId不能为空，请在配置文件中指定：spark.rocket.group.id$keyNum")

    // 详细的RocketMQ配置信息
    val finalRocketParam = RocketMQUtils.rocketParams(rocketParam, finalGroupId, rocketNameServer = null, tag = tag, keyNum)
    require(!finalRocketParam.isEmpty, "RocketMQ相关配置不能为空！")
    require(finalRocketParam.containsKey(RocketMQConfig.NAME_SERVER_ADDR), s"RocketMQ nameserver.addr不能为空，请在配置文件中指定：spark.rocket.brokers.name$keyNum")
    require(finalRocketParam.containsKey(RocketMQConfig.CONSUMER_TAG), s"RocketMQ tag不能为空，请在配置文件中指定：spark.rocket.consumer.tag$keyNum")

    RocketMqUtils.createMQPullStream(this.ssc,
      finalGroupId,
      JavaConversions.asJavaCollection(finalTopics.split(",").toList),
      finalConsumerStrategy,
      finalAutoCommit,
      forceSpecial = FireRocketMQConf.rocketForceSpecial(keyNum),
      failOnDataLoss = FireRocketMQConf.rocketFailOnDataLoss(keyNum),
      locationStrategy, finalRocketParam)
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