package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.spark.BaseSparkStreaming


/**
 * 基于Fire进行Spark Streaming开发
 */
object Test extends BaseSparkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print
    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
