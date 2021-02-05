package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.{DateFormatUtils, ExceptionBus, OSUtils}
import com.zto.fire.spark.BaseSparkStreaming
import com.zto.fire.spark.util.SparkUtils


/**
 * 基于Fire进行Spark Streaming开发
 */
object Test extends BaseSparkStreaming {
  /*@Scheduled(fixedInterval = 10000, scope = "executor", initialDelay = 30000L)
  def collectException: Unit = {

    println("----collectException")
  }

  @Scheduled(fixedInterval = 10000, scope = "driver", initialDelay = 30000L)
  def showException: Unit = {
    /*val queue = this.acc.getLog
    queue.foreach(log => println(log))
    println("----showException")*/
    println("累加值：" + this.acc.getCounter)
  }*/

  override def process: Unit = {
    (1 to 1000).foreach(count => {
      this.fire.createRDD(1 to 1000, 10).foreachPartition(it => {
        tryWithLog {
          this.acc.addCounter(1)
          val a = 1 / 0
        } (isThrow = false)
      })
      Thread.sleep(10000)
    })
    spark.sql(
      """
        |create table tmp.xxl as
        |select *, case when (channel like '%微信%' or channel like '%支付宝%') then 1
        |when channel not like '%微信%' and channel not like '%支付宝%' then 2 else 0 end  as channel_index
        |from ml.arrive_t_feature_sum_spark_total_bak where ds='20201227'
        |and rec_site_id<>0 and disp_site_id<>0 and send_city_id is not null and receiv_city_id is not null
        |and earliest2disp2rec_hour_diff>=16 and earliest2disp2rec_hour_diff<=144 DISTRIBUTE BY rand()
        |""".stripMargin).show(100, false)
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
