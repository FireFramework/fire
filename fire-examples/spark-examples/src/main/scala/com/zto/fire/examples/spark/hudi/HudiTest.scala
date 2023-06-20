package com.zto.fire.examples.spark.hudi

import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector.{Hive, Hudi, RocketMQ}
import com.zto.fire.spark.HudiStreaming
import com.zto.fire.spark.anno.Streaming

@Config(
  value = """
            |hudi.tableName     =   hudi.student
            |hudi.primaryKey    =   id
            |hudi.precombineKey =   createTime
            |hudi.partition     =   ds
            |""", files = Array("hudi.properties"))
@Hive(cluster = "fat")
@Hudi(parallelism = 10, compactCommits = 5)
@Streaming(interval = 10, backpressure = false, parallelism = 10)
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire1")
object HudiTest extends HudiStreaming {

  /**
   * 将查询语句的结果集插入到指定的hudi表中
   *
   * @param tmpView
   * 每个批次的消息注册成的临时表名
   */
  override protected def sqlUpsert(tmpView: String): String = {
    s"""
       |select
       | id,
       | name,
       | age,
       | createTime,
       | name as data,
       | name as data2,
       |	regexp_replace(substr(createTime,0,10),'-','') ds
       |from $tmpView
       |""".stripMargin
  }

  /**
   * hudi建表语句，将自动被执行
   * 注：该方法非必须
   */
  override protected def sqlCreate(tableName: String): String = {
    s"""
      |DROP TABLE IF EXISTS $tableName;
      |CREATE TABLE IF NOT EXISTS $tableName (
      |  `id` BIGINT,
      |  `name` STRING,
      |  `age` INT,
      |  `createTime` STRING,
      |  `data` STRING,
      |  `data2` STRING,
      |  `ds` STRING)
      |USING hudi
      |OPTIONS (
      |  `primaryKey` 'id',
      |  `type` 'mor',
      |  `preCombineField` 'createTime')
      |PARTITIONED BY (ds);
      |""".stripMargin
  }

  /**
   * 删除hudi数据逻辑，根据指定的id与分区写delete语句或select语句
   * 注：该方法非必须
   *
   * @param tmpView
   * 每个批次的消息注册成的临时表名
   */
  override protected def sqlDelete(tmpView: String): String = {
    s"""
      |select
      | id,
      | regexp_replace(substr(createTime,0,10),'-','') ds
      |from $tmpView where id=2
      |""".stripMargin
  }
}

