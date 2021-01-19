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
    val value = new JHashMap[String, String]()

    this.fire.start
  }

  def show(value: JInt, str: String, any: JHashMap[String, String]): Unit = {
    requireNonEmpty(value, str, any)
  }

  def main(args: Array[String]): Unit = {
    // this.init(10, false)
    show(1, "h", new JHashMap[String, String]())

  }
}
