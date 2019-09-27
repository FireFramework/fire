package com.zto.fire.demo

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.bean.RuntimeInfo
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

import scala.collection.JavaConversions

object Test extends BaseSparkStreaming {

  override def process: Unit = {
    println("====================================")
    println(JSON.toJSONString(RuntimeInfo.getRuntimeInfo, SerializerFeature.NotWriteRootClassName))
    println("====================================")
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD(rdd => {
      rdd.foreach(t => t.value().length)
    })
    this.runAsSchedule(this.collectLog, 1, 1)
    this.ssc.startAwaitTermination()
  }

  /**
   * 日志收集
   */
  def collectLog: Unit = {
    JavaConversions.asScalaIterator(this.acc.getLog.iterator()).foreach(t => {
      println(t)
    })
    this.acc.logAccumulator.reset()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
