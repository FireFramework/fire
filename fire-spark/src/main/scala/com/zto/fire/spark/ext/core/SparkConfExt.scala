package com.zto.fire.spark.ext.core

import com.zto.fire.common.conf.FireSparkConf
import com.zto.fire.spark.util.SparkUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf

/**
  * SparkConf扩展
  *
  * @param sparkConf
  * sparkConf对象
  * @author ChengLong 2019-5-18 10:50:35
  */
class SparkConfExt(sparkConf: SparkConf) {

  /**
    * 启用并注册kryo序列化
    */
  @deprecated
  def kryoRegister(clazz: Class[_]*): SparkConf = {
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialization")
    sparkConf.registerKryoClasses(clazz.toArray)
    sparkConf
  }

  /**
    * 设置Streaming默认配置
    *
    * @return
    * SparkConf实例
    */
  @deprecated
  def setStreamingDefault(): SparkConf = {
    sparkConf.set("spark.speculation", "true")
      .set("spark.streaming.concurrentJobs", "3")
      .set("spark.default.parallelism", "100")
      .set("spark.speculation.interval", "1000ms")
      .set("spark.speculation.multiplier", "1.8")
      .set("spark.speculation.quantile", "0.1")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.port.maxRetries", "1000")
    sparkConf
  }

  /**
    * 设置默认配置
    *
    * @return
    * SparkConf实例
    */
  @deprecated
  def setDefault(): SparkConf = {
    sparkConf.set("spark.broadcast.compress", "true")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec")
      .set("spark.reducer.maxSizeInFlight", "96")
      .set("spark.shuffle.io.maxRetries", "60")
      .set("spark.shuffle.io.retryWait", "60")
      .set("spark.port.maxRetries", "1000")
    sparkConf
  }

  /**
    * 设置名称和配置
    *
    * @return
    * SparkConf实例
    */
  @deprecated
  def buildConf(): SparkConf = {
    sparkConf.setAppName(FireSparkConf.appName)
    if (SparkUtils.isLocal) {
      sparkConf.setMaster("local[10]")
    }

    val props = FireSparkConf.sparkConf
    if (StringUtils.isNotBlank(props)) {
      val propArr = props.split("#")
      if (propArr != null && propArr.length > 0) {
        propArr.foreach(prop => {
          if (StringUtils.isNotBlank(prop)) {
            val confArr = prop.split(",")
            if (confArr != null && confArr.length == 2) {
              sparkConf.set(confArr(0), confArr(1))
            }
          }
        })
      }
    }
    sparkConf
  }
}
