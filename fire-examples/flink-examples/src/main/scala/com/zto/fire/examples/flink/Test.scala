/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.examples.flink.sql.SimpleSqlDemo.sql
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.streaming.api.scala._

@Config(
  """
    |flink.checkpoint.adaptive.enable=true
    |flink.checkpoint.adaptive.delay_start=20
    |flink.checkpoint.adaptive.active_trigger.interval=16:50~16:56, 10:46~10:47
    |flink.checkpoint.adaptive.active_trigger.duration=20000
    |fire.lineage.debug.enable=false
    |fire.debug.class.code.resource=org.apache.hadoop.hbase.client.Put
    |""")
@Streaming(interval = 10, disableOperatorChaining = true, parallelism = 2)
@Kafka(brokers = "bigdata_test", topics = "fire2", groupId = "fire2", forceOverwriteStateOffset = true, startingOffset = "latest")
@Kafka2(brokers = "bigdata_test", topics = "fire", groupId = "fire2")
object Test extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    sql(
      """
        |CREATE TABLE t_student (
        |  name STRING,
        |  age INT
        |) WITH (
        |  'connector' = 'kafka',								-- 用于指定connector的类型
        |  'topic' = 'fire',										-- 消费的topic名称为fire
        |  'properties.bootstrap.servers' = 'kafka-server:9092',	-- kafka的broker地址
        |  'properties.group.id' = 'fire3',						-- 当前flink sql任务所使用的groupId
        |  'scan.startup.mode' = 'earliest-offset',				-- 指定从什么位置开始消费
        |  'properties.kafka.force.autoCommit.enable' = 'true',	-- 指定是否自动提交offset
        |  'properties.kafka.force.autoCommit.Interval' = '10000',
        |  'format' = 'json'										-- 指定解析的kafka消息为json格式
        |)
        |""".stripMargin)

    sql(
      """
        |create table t_print(
        |  name STRING,
        |  age INT
        |) with (
        |  'connector' = 'print'
        |)
        |""".stripMargin)

    sql(
      """
        |insert into t_print select * from t_student
        |""".stripMargin)
  }
}