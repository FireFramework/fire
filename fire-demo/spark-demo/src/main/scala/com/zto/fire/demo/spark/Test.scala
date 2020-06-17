package com.zto.fire.demo.spark

import com.zto.fire.common.db.HBaseOper
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

object Test extends BaseSparkStreaming {

  override def process: Unit = {
    HBaseOper.scan("test", HBaseOper.buildScan("0", "1"))
    this.spark.parallelize(1 to 10).foreach(i => {
      HBaseOper.scan("test", HBaseOper.buildScan("0", "1"))
    })
    // val dstream = this.ssc.createDirectStream()
    /*dstream.mapOgg(classOf[Student]).foreachRDD(rdd => {
      rdd.foreach(t => println(t.getTable + " " + t.getBefore.getId + " " + t.getAfter.getName + " rowkey=" + t.getBefore.getRowKey + " " + t.getAfter.getRowKey))
    })*/
    // dstream.print()
    // this.ssc.startAwaitTermination()
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
    this.stop
  }
}
