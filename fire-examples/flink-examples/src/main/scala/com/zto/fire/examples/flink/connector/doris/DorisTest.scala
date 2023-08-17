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

package com.zto.fire.examples.flink.connector.doris

import com.zto.fire._
import com.zto.fire.common.util.{JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

@Streaming(interval = 20, disableOperatorChaining = true, parallelism = 2, unaligned = false)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object DorisTest extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    sql(
      """
        |CREATE table source (
        |  id int,
        |  name string,
        |  age int,
        |  createTime string
        |) with (
        | 'connector'='fire-rocketmq',
        | 'format'='json',
        | 'rocket.brokers.name'='bigdata_test',
        | 'rocket.topics'='fire',
        | 'rocket.group.id'='fire11',
        | 'rocket.consumer.tag'='*'
        |)
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE flink_doris_sink (
        |  id int,
        |  name string,
        |  age int,
        |  createTime string
        |) WITH (
        |  'connector' = 'doris',
        |  'datasource' = 'doris_test', -- 配置信息在common.properties中，可以包括path、thrift url等敏感数据源信息
        |  'table.identifier' = 'dpa_analysis.doris_flink_test',
        |  'username' = 'root',
        |  'sink.label-prefix' = 'doris_label'
        |)
        |""".stripMargin)

    sql(
      """
        |insert into flink_doris_sink
        |select
        |   id, name, age, createTime
        |from source
        |""".stripMargin)

    ThreadUtils.scheduleAtFixedRate({
      println(s"\n\n" + JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue))
    }, 0, 20, TimeUnit.SECONDS)
  }

}