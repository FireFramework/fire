package com.zto.fire.demo.flink.stream

import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.api.scala._

object KafkaDemo extends BaseFlinkStreaming{

  def main(args: Array[String]): Unit = {
     this.init()
  }

  override def process(): Unit = {
    val kafkaStream = this.env.createDirectStream()
    kafkaStream.map(t => {
      logger.error("log=>" + t)
      t.length
    }).print
    this.env.startAwaitTermination()
  }

}
