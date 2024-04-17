/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.spark.ext.core

import com.zto.fire._
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.ConsumerOffsetInfo
import com.zto.fire.common.conf.{FireFrameworkConf, FireKafkaConf, FireRocketMQConf, KeyNum}
import com.zto.fire.common.enu.Datasource.{KAFKA, ROCKETMQ}
import com.zto.fire.common.enu.{Operation => FOperation}
import com.zto.fire.common.lineage.parser.connector.{KafkaConnectorParser, RocketmqConnectorParser}
import com.zto.fire.common.util.Logging
import com.zto.fire.spark.util.{SparkRocketMQUtils, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy, RocketMQConfig, RocketMqUtils}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils

import java.util.Collections

/**
 * StreamingContext扩展
 *
 * @param ssc
 * StreamingContext对象
 * @author ChengLong 2019-5-18 11:03:59
 */
class StreamingContextExt(ssc: StreamingContext) extends Logging {

  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
  import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

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
  def createDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, groupId: String = null, consumerOffsetInfo: Set[ConsumerOffsetInfo] = Set.empty, keyNum: Int = KeyNum._1): DStream[ConsumerRecord[String, String]] = {
    // kafka topic优先级：配置文件 > topics参数
    val confTopic = FireKafkaConf.kafkaTopics(keyNum)
    val finalKafkaTopic = if (StringUtils.isNotBlank(confTopic)) SparkUtils.topicSplit(confTopic) else topics
    require(finalKafkaTopic != null && finalKafkaTopic.nonEmpty, s"kafka topic不能为空，请在配置文件中指定：spark.kafka.topics$keyNum")
    logInfo(s"kafka topic is $finalKafkaTopic")

    val confKafkaParams = com.zto.fire.common.util.KafkaUtils.kafkaParams(kafkaParams, groupId, keyNum = keyNum)
    require(confKafkaParams.nonEmpty, "kafka相关配置不能为空！")
    require(confKafkaParams.contains("bootstrap.servers"), s"kafka bootstrap.servers不能为空，请在配置文件中指定：spark.kafka.brokers.name$keyNum")
    require(confKafkaParams.contains("group.id"), s"kafka group.id不能为空，请在配置文件中指定：spark.kafka.group.id$keyNum")

    val finalGroupId = confKafkaParams("group.id").toString

    // kafka消费信息埋点
    KafkaConnectorParser.addDatasource(KAFKA, confKafkaParams("bootstrap.servers").toString, finalKafkaTopic.mkString("", ", ", ""), finalGroupId, FOperation.SOURCE)

    // 获取指定的消费位点信息
    val offsets = getKafkaTopicPartition(consumerOffsetInfo, finalGroupId, keyNum)

    KafkaUtils.createDirectStream[JString, JString](
      ssc, PreferConsistent, Subscribe[JString, JString](finalKafkaTopic, confKafkaParams, offsets))
  }

  /**
   * 根据优先级获取消费kafka的位点信息
   */
  @Internal
  private[this] def getKafkaTopicPartition(consumerOffsetInfo: Set[ConsumerOffsetInfo], groupId: String, keyNum: Int): JMap[TopicPartition, JLong] = {
    val confOffsetJson = FireFrameworkConf.consumerInfo(keyNum)
    if (isEmpty(confOffsetJson) && isEmpty(consumerOffsetInfo)) return Collections.emptyMap()

    tryWithReturn {
      // 配置文件优先级高于代码硬编码
      val offsets = new JHashMap[TopicPartition, JLong]

      // 从配置文件中获取消费位点信息
      val confOffsets = ConsumerOffsetInfo.jsonToBean(confOffsetJson)
      if (confOffsets.isEmpty) {
        if (noEmpty(consumerOffsetInfo)) {
          // 如果配置文件中未指定并且代码中指定了，则以代码传参为准
          offsets.putAll(ConsumerOffsetInfo.toKafkaTopicPartition(consumerOffsetInfo))
        }
      } else {
        // 如果配置文件中指定了消费位点信息，则从配置文件获取
        confOffsets.foreach(info => {
          if (!groupId.equalsIgnoreCase(info.getTopic)) {
            // 合法性检查：json中指定的groupId必须与当前任务配置的groupId保持一致
            throw new IllegalArgumentException(s"用于重置消费位点的groupId=${info.getTopic}与任务中配置的groupId=${groupId}不一致，请检查！")
          }
        })

        offsets.putAll(ConsumerOffsetInfo.toKafkaTopicPartition(confOffsets))
      }

      offsets
    } (this.logger, catchLog = s"获取消费位点信息失败，请检查配置参数，${FireFrameworkConf.FIRE_CONSUMER_OFFSET_INFO}=$confOffsetJson")
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
  def createRocketPullStream(rocketParam: JMap[String, String] = null,
                             groupId: String = this.appName,
                             topics: String = null,
                             tag: String = null,
                             consumerStrategy: ConsumerStrategy = ConsumerStrategy.lastest,
                             locationStrategy: LocationStrategy = LocationStrategy.PreferConsistent,
                             instance: String = "",
                             keyNum: Int = KeyNum._1): InputDStream[MessageExt] = {

    // 获取topic信息，配置文件优先级高于代码中指定的
    val confTopics = FireRocketMQConf.rocketTopics(keyNum)
    val finalTopics = if (StringUtils.isNotBlank(confTopics)) confTopics else topics
    require(StringUtils.isNotBlank(finalTopics), s"RocketMQ的Topics不能为空，请在配置文件中指定：spark.rocket.topics$keyNum")

    // 起始消费位点
    val confOffset = FireRocketMQConf.rocketStartingOffset(keyNum)
    val finalConsumerStrategy = if (StringUtils.isNotBlank(confOffset)) SparkRocketMQUtils.valueOfStrategy(confOffset) else consumerStrategy

    // 是否自动提交offset
    val finalAutoCommit = FireRocketMQConf.rocketEnableAutoCommit(keyNum)

    // groupId信息
    val confGroupId = FireRocketMQConf.rocketGroupId(keyNum)
    val finalGroupId = if (StringUtils.isNotBlank(confGroupId)) confGroupId else groupId
    require(StringUtils.isNotBlank(finalGroupId), s"RocketMQ的groupId不能为空，请在配置文件中指定：spark.rocket.group.id$keyNum")

    // 详细的RocketMQ配置信息
    val finalRocketParam = SparkRocketMQUtils.rocketParams(rocketParam, finalGroupId, rocketNameServer = null, tag = tag, keyNum)
    require(!finalRocketParam.isEmpty, "RocketMQ相关配置不能为空！")
    require(finalRocketParam.containsKey(RocketMQConfig.NAME_SERVER_ADDR), s"RocketMQ nameserver.addr不能为空，请在配置文件中指定：spark.rocket.brokers.name$keyNum")
    require(finalRocketParam.containsKey(RocketMQConfig.CONSUMER_TAG), s"RocketMQ tag不能为空，请在配置文件中指定：spark.rocket.consumer.tag$keyNum")

    // 消费者标识
    val instanceId = FireRocketMQConf.rocketInstanceId(keyNum)
    val finalInstanceId = if (StringUtils.isNotBlank(instanceId)) instanceId else instance
    if (StringUtils.isNotBlank(finalInstanceId)) finalRocketParam.put("consumer.instance", finalInstanceId)

    // 消费rocketmq埋点信息
    RocketmqConnectorParser.addDatasource(ROCKETMQ, finalRocketParam(RocketMQConfig.NAME_SERVER_ADDR), finalTopics, finalGroupId, FOperation.SOURCE)

    RocketMqUtils.createMQPullStream(this.ssc,
      finalGroupId,
      finalTopics.split(",").toList,
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

  /**
   * 提交Spark Streaming Graph并执行
   */
  def start: Unit = this.startAwaitTermination()
}