package com.zto.fire.demo.spark

import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._

object Test extends BaseSparkStreaming {

  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    dstream.print()
    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
     this.init(10, false)
  }
}
