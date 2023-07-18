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

package com.zto.fire.spark.util

import com.zto.fire._
import com.zto.fire.common.conf.{FireRocketMQConf, KeyNum}
import com.zto.fire.common.util.{KafkaUtils, LogUtils, Logging, StringsUtils}
import com.zto.fire.predef.{noEmpty, requireNonEmpty}
import org.apache.commons.lang3.StringUtils
import org.apache.rocketmq.spark.{ConsumerStrategy, RocketMQConfig}

import scala.collection.mutable

/**
 * RocketMQ相关工具类
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-06-29 10:50
 */
object RocketMQUtils extends Logging {

  /**
   * rocketMQ配置信息
   *
   * @param groupId
   * 消费组
   * @return
   * rocketMQ相关配置
   */
  def rocketParams(rocketParam: JMap[String, String] = null,
                   groupId: String = null,
                   rocketNameServer: String = null,
                   tag: String = null,
                   keyNum: Int = KeyNum._1): JMap[String, String] = {

    val optionParams = if (rocketParam != null) rocketParam else new JHashMap[String, String]()
    if (StringUtils.isNotBlank(groupId)) optionParams.put(RocketMQConfig.CONSUMER_GROUP, groupId)

    // rocket name server 配置
    val confNameServer = FireRocketMQConf.rocketNameServer(keyNum)
    val finalNameServer = if (StringUtils.isNotBlank(confNameServer)) confNameServer else rocketNameServer
    if (StringUtils.isNotBlank(finalNameServer)) optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, finalNameServer)

    // tag配置
    val confTag = FireRocketMQConf.rocketConsumerTag(keyNum)
    val finalTag = if (StringUtils.isNotBlank(confTag)) confTag else tag
    if (StringUtils.isNotBlank(finalTag)) optionParams.put(RocketMQConfig.CONSUMER_TAG, finalTag) else optionParams.put(RocketMQConfig.CONSUMER_TAG, RocketMQConfig.DEFAULT_TAG)

    // 每个分区拉取的消息数
    val maxSpeed = FireRocketMQConf.rocketPullMaxSpeedPerPartition(keyNum)
    if (StringUtils.isNotBlank(maxSpeed) && StringsUtils.isInt(maxSpeed)) optionParams.put(RocketMQConfig.MAX_PULL_SPEED_PER_PARTITION, maxSpeed)

    // 以spark.rocket.conf.开头的配置优先级最高
    val confMap = FireRocketMQConf.rocketConfMap(keyNum)
    if (confMap.nonEmpty) optionParams.putAll(confMap)
    // 日志记录RocketMQ的配置信息
    LogUtils.logMap(this.logger, optionParams.toMap, s"RocketMQ configuration. keyNum=$keyNum.")

    optionParams
  }

  /**
   * 根据KeyNum获取配置的kafka数据源信息
   *
   * @param url
   * kafka集群url
   * @param topic
   * kafka topic
   * @param kafkaParams
   * kafka参数
   * @param keyNum
   * 配置的数值后缀
   * @return
   */
  def getConfByKeyNum(url: String = null,
                      topic: String = null,
                      tag: String = null,
                      rocketParam: Map[String, Object] = null,
                      keyNum: Int = KeyNum._1): (String, String, String, Map[String, String]) = {
    // rocket name server 配置
    val confNameServer = FireRocketMQConf.rocketNameServer(keyNum)
    val finalUrl = if (StringUtils.isNotBlank(confNameServer)) confNameServer else url
    requireNonEmpty(finalUrl)(s"消息队列url不能为空，请检查keyNum=${keyNum}对应的配置信息")

    // tag配置
    val confTag = FireRocketMQConf.rocketConsumerTag(keyNum)
    val finalTag = if (StringUtils.isNotBlank(confTag)) confTag else if (isEmpty(tag)) "*" else tag

    // topic配置
    val confTopic = FireRocketMQConf.rocketTopics(keyNum)
    val finalTopic = if (noEmpty(confTopic)) confTopic else topic
    requireNonEmpty(finalTopic)(s"消息队列topic不能为空，请检查keyNum=${keyNum}对应的配置信息")

    // 以spark.rocket.conf.开头的配置优先级最高
    val confMap = FireRocketMQConf.rocketConfMap(keyNum)
    val finalConf = new collection.mutable.HashMap[String, String]()
    if (noEmpty(rocketParam)) {
      rocketParam.foreach(kv => finalConf.put(kv._1, kv._2.toString))
    }

    if (noEmpty(confMap)) {
      confMap.foreach(kv => finalConf.put(kv._1, kv._2))
    }

    (finalUrl, finalTopic, finalTag, finalConf.toMap)
  }

  /**
   * 根据消费位点字符串获取ConsumerStrategy实例
   * @param offset
   *               latest/earliest
   */
  def valueOfStrategy(offset: String): ConsumerStrategy = {
    if ("latest".equalsIgnoreCase(offset)) {
      ConsumerStrategy.lastest
    } else {
      ConsumerStrategy.earliest
    }
  }

}
