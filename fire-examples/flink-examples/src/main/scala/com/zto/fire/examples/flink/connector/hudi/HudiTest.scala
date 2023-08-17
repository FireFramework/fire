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

package com.zto.fire.examples.flink.connector.hudi

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
object HudiTest extends FlinkStreaming {

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
        |create table if not exists `t_hudi_flink`(
        |  id int,
        |  name string,
        |  age int,
        |  createTime string,
        |  ds string
        |) PARTITIONED BY (`ds`)
        |with(
        |  'connector'='hudi',
        |  'datasource' = 'hudi_test', -- 配置信息在common.properties中，可以包括path、thrift url等敏感数据源信息
        |  'table.type'='MERGE_ON_READ',
        |  'hoodie.datasource.write.recordkey.field'='id',
        |  'precombine.field'='createTime',
        |  'hive_sync.enable'='true',
        |  'hive_sync.db'='hudi',
        |  'hive_sync.table'='t_hudi_flink',
        |  'hive_sync.mode'='hms',
        |  'hoodie.datasource.write.hive_style_partitioning'='true',
        |  'hive_sync.partition_fields'='ds',
        |  'compaction.async.enabled'='false',
        |  'compaction.schedule.enabled'='true',
        |  'compaction.trigger.strategy'='num_commits',
        |  'compaction.delta_commits'='2',
        |  'index.type'='BUCKET',
        |  'hoodie.bucket.index.num.buckets'='2'
        |)
        |""".stripMargin)

    sql(
      """
        |insert into t_hudi_flink
        |select
        |   id, name, age, createTime, regexp_replace(substr(createTime,0,10),'-','') ds
        |from source
        |""".stripMargin)

    ThreadUtils.scheduleAtFixedRate({
      println(s"\n\n\n" + JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue))
    }, 0, 20, TimeUnit.SECONDS)
  }

}