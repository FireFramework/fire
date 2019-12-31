package com.zto.fire.demo

import java.util.Date

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.ext.module.KuduContextExt

object Test extends BaseSparkStreaming {

  /**
   * 每天23点为trans_fee与pangu这两张kudu表添加分区
   */
  @Scheduled(cron = "0 0 23 * * ?", concurrent = false)
  def setConf: Unit = {
    KuduContextExt.addPartition(Seq("trans_fee", "pangu"),
      // 明天
      DateFormatUtils.addPartitionDays(new Date(), 1),
      // 后天
      DateFormatUtils.addPartitionDays(new Date, 2))
  }


  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        println(s"============= start print conf ${it.size} ================")
        this.conf.getAll.foreach(c => println(c._1 + " " + c._2))
        println("============= end print conf ================")
      })
    })
    this.ssc.startAwaitTermination()
  }


  def main(args: Array[String]): Unit = {
    this.init(60, false)
  }
}
