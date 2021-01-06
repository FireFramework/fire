package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * 基于Fire进行Flink Streaming开发
 */
object Test extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print
    this.fire.start()
  }


  def main(args: Array[String]): Unit = {
    this.init()
  }
}
