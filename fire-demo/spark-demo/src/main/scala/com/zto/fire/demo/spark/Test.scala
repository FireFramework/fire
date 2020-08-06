package com.zto.fire.demo.spark

import com.zto.fire.common.util.PropUtils
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

object Test extends BaseSparkStreaming {
  val key = "fire.partitions"

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()

    dstream.foreachRDD(rdd => {
      rdd.repartition(this.conf.get(key, "10").toInt).foreachPartition(it => {
        println("conf=" + this.conf.get(key, "10") + " PropUtils=" + PropUtils.getString(key))
      })
    })

    this.ssc.startAwaitTermination()
  }


  def main(args: Array[String]): Unit = {
     this.init(10, false)
  }
}
