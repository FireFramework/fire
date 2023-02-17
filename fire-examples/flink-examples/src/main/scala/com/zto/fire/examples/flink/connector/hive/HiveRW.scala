package com.zto.fire.examples.flink.connector.hive

import com.zto.fire._
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.flink.FlinkStreaming

/**
 * 基于Fire进行Flink Streaming开发
 */
@Hive("test")
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire", autoCommit = true)
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HiveRW extends FlinkStreaming {

  /**
   * 业务逻辑代码，会被fire自动调用
   */
  override def process: Unit = {
    this.fire.useHiveCatalog()

    this.ddl

    sql(
      """
        |insert into table tmp.baseorganize_fire select * from dim.baseorganize limit 10
        |""".stripMargin)

    sql(
      """
        |select * from tmp.baseorganize_fire
        |""".stripMargin).print()
  }

  /**
   * 创建表
   */
  def ddl: Unit = {
    sql(
      """
        |drop table if exists tmp.baseorganize_fire
        |""".stripMargin)

    sql(
      """
        |create table tmp.baseorganize_fire (
        |    id bigint,
        |    name string,
        |    age int
        |) partitioned by (ds string)
        |row format delimited fields terminated by '/t'
        |""".stripMargin)
  }
}
