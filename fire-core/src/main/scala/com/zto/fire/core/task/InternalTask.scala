package com.zto.fire.core.task

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.acc.AccumulatorManager
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.bean.runtime.RuntimeInfo
import com.zto.fire.common.util.{DateFormatUtils, EncryptUtils, HttpClientUtils, ValueUtils}
import com.zto.fire.core.BaseSpark
import org.apache.commons.httpclient.Header
import org.apache.spark.{Logging, SparkConf, SparkEnv}

import scala.collection.JavaConversions

/**
 * 定时任务调度器，用于定时执行fire框架内部指定的任务
 *
 * @author ChengLong 2019年11月5日 10:11:31
 */
private[fire] class InternalTask(baseSpark: BaseSpark) extends Logging with Serializable {
  // fire框架restful地址
  private var restful: String = _

  /**
   * 定时采集运行时的jvm、gc、thread、cpu、memory、disk等信息
   * 并将采集到的数据存放到EnvironmentAccumulator中
   */
  @Scheduled(fixedInterval = 5000, scope = "all", initialDelay = 0L, concurrent = false)
  def collectEnvironmentInfo: Unit = {
    AccumulatorManager.addEnv(JSON.toJSONString(RuntimeInfo.getRuntimeInfo, SerializerFeature.NotWriteRootClassName))
  }

  /**
   * 定时同步最新的conf信息到executor端
   */
  @Scheduled(cron = "/10 * * * * ?", scope = "executor", initialDelay = 5000L, concurrent = false)
  def syncConf: Unit = {
    // 获取driver的restful地址
    if (ValueUtils.isEmpty(this.restful) && SparkEnv.get != null) {
      this.restful = SparkEnv.get.conf.get("spark.rest.url") + "/system/getConf"
    }

    // 同步driver端的conf信息
    if (ValueUtils.isNotEmpty(this.restful)) {
      try {
        if (this.baseSpark.conf == null) this.baseSpark.conf = new SparkConf()
        val secret = EncryptUtils.md5Encrypt("($zto%-%fire$)" + this.baseSpark.className + DateFormatUtils.formatCurrentDate)
        val json = HttpClientUtils.doGet(this.restful, new Header("Content-Type", "application/json"), new Header("Authorization", secret))
        if (ValueUtils.isNotEmpty(json)) {
          val confMap = JSON.parseObject(json, classOf[java.util.HashMap[String, String]])
          if (ValueUtils.isNotEmpty(confMap)) {
            this.baseSpark.conf.setAll(JavaConversions.mapAsScalaMap(confMap))
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

}
