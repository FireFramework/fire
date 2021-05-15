package com.zto.fire.examples.flink.connector.bean

import com.zto.fire._
import com.zto.fire.flink.BaseFlinkStreaming

/**
 * Flink流式计算任务模板
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 */
object BeanConnectorTest extends BaseFlinkStreaming {

  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    this.fire.sql(
      """
        |CREATE table source (
        |  id bigint,
        |  name string,
        |  age int,
        |  length double,
        |  data DECIMAL(10, 5)
        |)
        |WITH
        |   (
        |   'connector' = 'bean',
        |   'table-name' = 'source',
        |   'duration' = '5000',
        |   'repeat-times' = '5'
        |   )
        |""".stripMargin)

    this.fire.sql(
      """
        |CREATE table sink (
        |  id bigint,
        |  name string,
        |  age int,
        |  length double,
        |  data DECIMAL(10, 5)
        |)
        |WITH
        |   (
        |   'connector' = 'bean',
        |   'table-name' = 'sink'
        |   )
        |""".stripMargin)
    this.fire.sql(
      """
        |insert into sink select * from source
        |""".stripMargin)
    dstream.print()
    this.fire.start
  }


  def main(args: Array[String]): Unit = {
    this.init()
  }
}
