package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.util.{JSONUtils, PropUtils, StringsUtils}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._

/**
 * Flink流式计算任务模板
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 */
object Test extends BaseFlinkStreaming {

  override def process: Unit = {
    /*this.fire.sql("""
                    |CREATE table source (
                    |  id bigint,
                    |  name string,
                    |  age int
                    |)
                    |WITH
                    |   (
                    |   'connector' = 'kafka',
                    |   'topic' = 'fire',
                    |   'properties.bootstrap.servers' = '10.9.46.111:9092',
                    |   'properties.group.id' = 'fire',
                    |   'format' = 'json',
                    |   'scan.startup.mode' = 'earliest-offset'
                    |   )
                    |""".stripMargin)

    this.fire.sql(
      """
        |select * from source
        |""".stripMargin).print()*/
    this.fire.createRocketMqPullStreamWithTag().printToErr("包含所有")
    this.fire.createRocketMqPullStreamWithKey().printToErr("包含key")
    this.fire.createRocketMqPullStream().printToErr("仅value")
    this.fire.start
  }



  def main(args: Array[String]): Unit = {
    this.init()
  }
}
