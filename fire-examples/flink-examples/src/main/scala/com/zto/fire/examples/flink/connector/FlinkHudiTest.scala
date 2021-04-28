package com.zto.fire.examples.flink.connector

import com.zto.fire.flink.BaseFlinkStreaming

object FlinkHudiTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {

    var sql =
      """
        |CREATE TABLE hudi_table_test(
        |  uuid VARCHAR(20),
        |  action VARCHAR(10),
        |  age INT,
        |  ts BIGINT,
        |  ds VARCHAR(20)
        |)
        |PARTITIONED BY (ds)
        |WITH (
        |  'connector' = 'hudi',
        |  'path' = 'hdfs:///user/flink/huditest/hudi_table_test',
        |  'table.type' = 'MERGE_ON_READ',
        |  'compaction.delta_commits' = '3',
        |  'compaction.delta_seconds' = '300',
        |  'hoodie.datasource.write.hive_style_partitioning' = 'true'
        |)
        |""".stripMargin

    this.tableEnv.executeSql(sql)

    sql =
      """
        |CREATE TABLE kafka_source_table (
        |  uuid VARCHAR(20),
        |  action VARCHAR(10),
        |  age INT,
        |  ts BIGINT,
        |  ds VARCHAR(20)
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'kafka_hudi_test',
        |  'properties.bootstrap.servers' = '10.9.46.111:9092',
        |  'properties.group.id' = 'testGroup',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json'
        |)
        |""".stripMargin

    this.tableEnv.executeSql(sql)

    sql =
      """
        |INSERT INTO hudi_table_test SELECT uuid,action,age,ts,ds FROM kafka_source_table
        |""".stripMargin

    this.tableEnv.executeSql(sql)

  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}