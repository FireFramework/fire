package com.zto.fire.demo.flink.stream

import com.zto.fire.common.util.PropUtils
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.ext.FlinkExt._
import com.zto.fire.flink.core.util.FlinkUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConversions._

object KafkaDemo extends BaseFlinkStreaming {

  def main(args: Array[String]): Unit = {
    this.init()
  }

  override def process(): Unit = {
    val kafkaStream = this.env.createDirectStream()

    kafkaStream.map(new RichMapFunction[String, Int] {
      override def open(parameters: Configuration): Unit = {
        // val map = this.getRuntimeContext.getExecutionConfig.getGlobalJobParameters.toMap
        val map = PropUtils.toFlinkConfMap
        logger.error("execute open method. size=" + map.size)
        map.foreach(kv => {
          logger.error(s"open config: key=${kv._1} value=${kv._2}")
        })
      }

      override def map(in: String): Int = in.length
    }).print
    this.env.startAwaitTermination()
  }

}
