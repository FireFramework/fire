package com.zto.fire.demo.spark

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.Student

object Test extends BaseSparkStreaming {

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.mapOgg(classOf[Student]).foreachRDD(rdd => {
      rdd.foreach(t => println(t.getTable + " " + t.getBefore.getId + " " + t.getAfter.getName + " rowkey=" + t.getBefore.getRowKey + " " + t.getAfter.getRowKey))
    })
    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
    this.stop
  }
}
