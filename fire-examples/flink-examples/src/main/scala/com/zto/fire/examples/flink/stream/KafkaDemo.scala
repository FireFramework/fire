package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object KafkaDemo extends BaseFlinkStreaming {

  def main(args: Array[String]): Unit = {
    this.init()
  }

  override def process(): Unit = {
    val kafkaStream = this.env.createDirectStream()

    kafkaStream.map(new RichMapFunction[String, Int] {
      override def open(parameters: Configuration): Unit = {
        val map = this.getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap
        logger.error("execute open method. size=" + map.size)
        map.foreach(kv => {
          logger.error(s"open config: key=${kv._1} value=${kv._2}")
        })
      }

      override def map(in: String): Int = in.length
    }).print
    this.fire.start()
  }

}
