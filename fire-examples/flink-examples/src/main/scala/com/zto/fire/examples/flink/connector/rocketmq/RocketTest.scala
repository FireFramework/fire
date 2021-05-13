package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * Flink流式计算任务消费rocketmq
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-5-13 14:26:24
 */
object RocketTest extends BaseFlinkStreaming {

  override def process: Unit = {
    this.fire.createRocketMqPullStreamWithTag()
    this.fire.createRocketMqPullStreamWithKey()
    this.fire.createRocketMqPullStream()

    // 从另一个rocketmq中消费数据
    this.fire.createRocketMqPullStream(keyNum = 2)
    this.fire.start
  }



  def main(args: Array[String]): Unit = {
    this.init()
  }
}
