package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * Flink流式计算任务模板
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 */
object RocketMQConnectorTest extends BaseFlinkStreaming {

  override def process: Unit = {
    this.fire.sql("""
                    |CREATE table source (
                    |  id bigint,
                    |  name string,
                    |  age int,
                    |  length double,
                    |  data DECIMAL(10, 5)
                    |)
                    |WITH
                    |   (
                    |   'connector' = 'fire-rocketmq',
                    |   'format' = 'json',
                    |   'topic' = 'fire',
                    |   'properties.nameserver.address' = 'localhost:9876',
                    |   'properties.consumer.group' = 'fire'
                    |   )
                    |""".stripMargin)

    this.fire.sql(
      """
        |select * from source
        |""".stripMargin).print()
  }



  def main(args: Array[String]): Unit = {
    this.init()
  }
}
