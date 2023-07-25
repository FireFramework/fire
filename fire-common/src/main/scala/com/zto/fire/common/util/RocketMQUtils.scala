package com.zto.fire.common.util

import com.zto.fire.common.conf.{FireRocketMQConf, KeyNum}
import com.zto.fire.predef._
import org.apache.commons.lang3.StringUtils

object RocketMQUtils {

  /**
   * 根据KeyNum获取配置的kafka数据源信息
   *
   * @param url
   * kafka集群url
   * @param topic
   * kafka topic
   * @param rocketParam
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

}
