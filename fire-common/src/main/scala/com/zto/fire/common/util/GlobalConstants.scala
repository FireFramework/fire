package com.zto.fire.common.util

import com.zto.fire.common.conf.FireConf

/**
 * 常量配置类
 * Created by ChengLong on 2016-11-22.
 */
@deprecated("use FireConf", "v1.0.0")
object GlobalConstants extends FireConf {
  @deprecated("use FireConf.KafkaConfig", "v1.0.0")
  val KafkaConf = this.kafkaConf

  @deprecated("use FireConf.hbaseConf.familyName", "v1.0.0")
  val familyName = this.hbaseConf.familyName
}