package com.zto.fire.demo

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

object Test extends BaseSparkStreaming {

  @Scheduled(fixedInterval = 5000, scope = "all", initialDelay = 0L, concurrent = false)
  def setConf: Unit = {
    println("=============setConf==================")
  }

  @Scheduled(fixedInterval = 5000, scope = "all", initialDelay = 0L, concurrent = false)
  def setConf2: Unit = {
    println("=============setConf2==================")
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
