package com.zto.fire.common.util

import org.apache.commons.lang3.StringUtils
import org.apache.rocketmq.spark.{ConsumerStrategy, RocketMQConfig}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions

/**
 * RocketMQ相关工具类
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-06-29 10:50
 */
object RocketUtils {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * rocketMQ配置信息
   *
   * @param groupId
   * 消费组
   * @return
   * rocketMQ相关配置
   */
  def rocketParams(rocketParam: java.util.Map[String, String] = null,
                   groupId: String = null,
                   rocketNameServer: String = null,
                   tag: String = null,
                   keyNum: Int = 1): java.util.Map[String, String] = {

    val optionParams = if (rocketParam != null) rocketParam else new java.util.HashMap[String, String]()
    if (StringUtils.isNotBlank(groupId)) optionParams.put(RocketMQConfig.CONSUMER_GROUP, groupId)

    // rocket name server 配置
    val confNameServer = GlobalConstants.RocketConf.rocketNameServer(keyNum)
    val finalNameServer = if (StringUtils.isNotBlank(confNameServer)) confNameServer else rocketNameServer
    if (StringUtils.isNotBlank(finalNameServer)) optionParams.put(RocketMQConfig.NAME_SERVER_ADDR, finalNameServer)

    // tag配置
    val confTag = GlobalConstants.RocketConf.rocketConsumerTag(keyNum)
    val finalTag = if (StringUtils.isNotBlank(confTag)) confTag else tag
    if (StringUtils.isNotBlank(finalTag)) optionParams.put(RocketMQConfig.CONSUMER_TAG, finalTag)

    // 每个分区拉取的消息数
    val maxSpeed = GlobalConstants.RocketConf.rocketPullMaxSpeedPerPartition(keyNum)
    if (StringUtils.isNotBlank(maxSpeed) && StringsUtils.isInt(maxSpeed)) optionParams.put(RocketMQConfig.MAX_PULL_SPEED_PER_PARTITION, maxSpeed)

    // 以spark.rocket.conf.开头的配置优先级最高
    val confMap = GlobalConstants.RocketConf.rocketConfMap(keyNum)
    if (confMap.nonEmpty) optionParams.putAll(JavaConversions.mapAsJavaMap(confMap))
    // 日志记录RocketMQ的配置信息
    LogUtils.logMap(this.logger, JavaConversions.mapAsScalaMap(optionParams).toMap, s"RocketMQ configuration. keyNum=$keyNum.")

    optionParams
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
