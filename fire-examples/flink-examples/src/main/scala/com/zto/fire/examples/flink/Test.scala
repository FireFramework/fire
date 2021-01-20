package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * Flink流式计算任务模板
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 */
class Test extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print
    this.fire.start
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
