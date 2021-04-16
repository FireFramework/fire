package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils

/**
 * 常量配置类
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:00
 */
private[fire] class FireConf {
  // 用于区分不同的流计算引擎类型
  private[fire] lazy val engine = PropUtils.engine

  // Fire框架相关配置
  val frameworkConf = FireFrameworkConf
  // kafka相关配置
  val kafkaConf = FireKafkaConf
  // rocketMQ相关配置
  val rocketMQConf = FireRocketMQConf
  // impala相关配置
  val kuduConf = FireKuduConf
  // 颜色预定义
  val ps1Conf = FirePS1Conf
  // hive相关配置
  val hiveConf = FireHiveConf
}

object FireConf extends FireConf