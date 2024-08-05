package com.zto.fire.examples.flink.connector.hive

import com.zto.fire._
import com.zto.fire.common.anno.Config
import org.apache.flink.api.scala._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

/**
 * 基于Fire进行Flink Streaming开发
 */
@Streaming(20)
@Hive(cluster = "fat", version = "2.3.9")
@Kafka(brokers = "bigdata_test", topics = "fire2", groupId = "fire")
@Config(
  """
    |flink.checkpoint.adaptive.enable=true
    |flink.checkpoint.adaptive.delay_start=20
    |flink.checkpoint.adaptive.active_trigger.interval=13:14~13:15, 14:00~15:00
    |flink.checkpoint.adaptive.active_trigger.duration=20000
    |fire.debug.class.code.resource=com.fasterxml.jackson.databind.JsonMappingException,com.fasterxml.jackson.databind.ObjectMapper
    |""")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HiveRW extends FlinkStreaming {

  /**
   * 业务逻辑代码，会被fire自动调用
   */
  @Process
  def kafkaSource: Unit = {
    val dstream = this.fire.createKafkaDirectStream().map(JSONUtils.parseObject[Student](_))
    dstream.createOrReplaceTempView("t_student")

    this.fire.useHiveCatalog()
    sql(
      """
        |create table if not exists tmp.t_student(
        |id bigint,
        |name string,
        |age int,
        |createTime string,
        |length decimal(10, 2),
        |sex boolean
        |) partitioned by (ds string)
        |stored as orc
        |TBLPROPERTIES (
        |  'sink.partition-commit.trigger' = 'process-time',
        |  'sink.partition-commit.delay' = '1 min',
        |  'sink.partition-commit.policy.kind' = 'metastore,success-file'
        |)
        |""".stripMargin)

    this.fire.useDefaultCatalog
    sql(
      """
        |insert into hive.tmp.t_student
        |select
        | id,
        | name,
        | age,
        | createTime,
        | length,
        | sex,
        | regexp_replace(substr(createTime,0,10),'-','') ds
        |from t_student
        |""".stripMargin)
  }
}
