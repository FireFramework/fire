package com.zto.fire.common.conf

import com.zto.fire.common.util.GlobalConstants.{DefaultVals, PropKeys}
import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang3.StringUtils

/**
 * RocketMQ相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:58
 */
class RocketConfiguration extends Enumeration {
  val rocketOffsetLargest = "latest"
  val rocketOffsetSmallest = "earliest"
  val rocketConsumerTag = "*"
  val rocketClusterMapConfStart = "spark.rocket.cluster.map."
  // 初始化kafka集群名称与地址映射
  private lazy val rocketClusterMap = PropUtils.sliceKeys(rocketClusterMapConfStart)
  val rocketConfStart = "spark.rocket.conf."

  // rocket-client配置信息
  def rocketConfMap(keyNum: Int = 1): collection.immutable.Map[String, String] = PropUtils.sliceKeysByNum(rocketConfStart, keyNum)

  // 获取消费位点
  def rocketStartingOffset(keyNum: Int = 1): String = PropUtils.getString(PropKeys.ROCKET_STARTING_OFFSET, keyNum, "")

  // 丢失数据时是否失败
  def rocketFailOnDataLoss(keyNum: Int = 1): Boolean = PropUtils.getBoolean(PropKeys.ROCKET_FAIL_ON_DATA_LOSS, keyNum, DefaultVals.rocketFailOnDataLoss)

  // spark.rocket.forceSpecial
  def rocketForceSpecial(keyNum: Int = 1): Boolean = PropUtils.getBoolean(PropKeys.ROCKET_FORCE_SPECIAL, keyNum, false)

  // enable.auto.commit
  def rocketEnableAutoCommit(keyNum: Int = 1): Boolean = PropUtils.getBoolean(PropKeys.ROCKET_ENABLE_AUTO_COMMIT, keyNum, DefaultVals.rocketEnableAutoCommit)

  // 获取rocketMQ 订阅的tag
  def rocketConsumerTag(keyNum: Int = 1): String = PropUtils.getString(PropKeys.ROCKET_CONSUMER_TAG, keyNum, "")

  // 获取groupId
  def rocketGroupId(keyNum: Int = 1): String = PropUtils.getString(PropKeys.ROCKET_GROUP_ID, keyNum, "")

  // 获取rocket topic列表
  def rocketTopics(keyNum: Int = 1): String = PropUtils.getString(PropKeys.ROCKET_TOPICS, keyNum, null)

  // 每次拉取每个partition的消息数
  def rocketPullMaxSpeedPerPartition(keyNum: Int = 1): String = PropUtils.getString(PropKeys.ROCKET_PULL_MAX_SPEED_PER_PARTITION, keyNum, "")

  // 获取rocketMQ name server 地址
  def rocketNameServer(keyNum: Int = 1): String = {
    val brokerName = PropUtils.getString(PropKeys.ROCKET_BROKERS_NAME, keyNum, "")
    val nameServiceAddress = if (StringUtils.isNotBlank(brokerName) && brokerName.contains(":")) {
      brokerName
    } else {
      this.rocketClusterMap.getOrElse(brokerName, "")
    }
    nameServiceAddress
  }
}
