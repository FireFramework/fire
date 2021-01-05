package com.zto.fire.examples.spark.streaming

import com.zto.fire._
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.spark.BaseSparkStreaming

/**
 * kafka json解析
 *
 * @author ChengLong 2019-6-26 16:52:58
 */
object KafkaTest extends BaseSparkStreaming {

  // 每天凌晨4点01将锁标志设置为false，这样下一个批次就可以先更新维表再执行sql
  @Scheduled(cron = "0 1 4 * * ?")
  def updateTableJob: Unit = this.lock.compareAndSet(true, false)

  // 用于缓存变更过的维表，只有当定时任务将标记设置为可更新时才会真正拉取最新的表
  def cacheTable: Unit = {
    // 加载完成维表以后上锁
    if (this.lock.compareAndSet(false, true)) {
      // cache维表逻辑
    }
  }

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD(rdd => {
      // 更新并缓存维表动作，具体要根据锁的标记判断是否执行
      this.cacheTable

      // 一、将json解析并注册为临时表，默认不cache临时表
      rdd.kafkaJson2Table("test", cacheTable = true)
      // toLowerDF表示将大写的字段转为小写
      this.spark.sql("select * from test").toLowerDF.show(1, false)
      /*this.spark.sql("select after.* from test").toLowerDF.show(1, false)
      this.spark.sql("select after.* from test where after.order_type=1").toLowerDF.show(1, false)*/

      // 二、直接将json按指定的schema解析（只解析after），fieldNameUpper=true表示按大写方式解析，并自动转为小写
      // rdd.kafkaJson2DF(classOf[OrderCommon], fieldNameUpper = true).show(1, false)
      // 递归解析所有指定的字段，包括before、table、offset等字段
      // rdd.kafkaJson2DF(classOf[OrderCommon], parseAll = true, fieldNameUpper = true, isMySQL = false).show(1, false)

      this.spark.uncache("test")
    })

    val dstream2 = this.ssc.createDirectStream(keyNum = 2)
    dstream2.print(1)
    val dstream3 = this.ssc.createDirectStream(keyNum = 3)
    dstream3.count().foreachRDD(rdd => {
      println("count=" + rdd.count())
    })
    dstream3.print(1)
    val dstream5 = this.ssc.createDirectStream(keyNum = 5)
    dstream5.print(1)

    this.fire.start
  }

  @Scheduled(fixedInterval = 60 * 1000, scope = "all")
  def loadTable: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}=================== 每分钟执行loadTable ===================")
    this._conf.getAll.foreach(conf => println(conf._1 + " -> " + conf._2))
  }

  @Scheduled(cron = "0 0 * * * ?")
  def loadTable2: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}=================== 每小时执行loadTable2 ===================")
  }

  @Scheduled(cron = "0 0 9 * * ?")
  def loadTable3: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}=================== 每天9点执行loadTable3 ===================")
  }


  def main(args: Array[String]): Unit = {
    this.init(10, false)
    this.stop
  }
}
