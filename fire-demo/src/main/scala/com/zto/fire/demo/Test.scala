package com.zto.fire.demo

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

object Test extends BaseSparkStreaming {

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
