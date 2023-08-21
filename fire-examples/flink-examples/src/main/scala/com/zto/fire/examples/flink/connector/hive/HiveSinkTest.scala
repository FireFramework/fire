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

package com.zto.fire.examples.flink.connector.hive

import com.zto.fire._
import com.zto.fire.common.util.{JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.Hive
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.sync.FlinkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

/**
 * 基于fire框架进行Flink SQL开发<br/>
 * <a href='https://www.bookstack.cn/read/ApacheFlink-1.12-zh/4b9179fa090b9360.md'>1. Flink SQL开发官方文档——kafka connector</a><br/>
 * <a href='https://www.bookstack.cn/read/ApacheFlink-1.12-zh/a7dfbfd1c259be68.md'>2. Flink SQL开发官方文档——jdbc connector</a>
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-18 17:24
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Hive("fat")
@Streaming(10)
object HiveSinkTest extends FlinkStreaming {

  // 具体的业务逻辑放到process方法中
  override def process: Unit = {
    this.fire.disableOperatorChaining()
    ThreadUtils.scheduleAtFixedRate({
      println(s"\n\n\n" + JSONUtils.toJSONString(FlinkLineageAccumulatorManager.getValue))
    }, 0, 20, TimeUnit.SECONDS)
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
    this.fire.useHiveCatalog()
    println(this.tableEnv.getCurrentCatalog)

    sql(
      """
        |CREATE TABLE if not exists tmp.zfs_upload_record_topic_fire (
        |  id int,
        |  name string,
        |  age int,
        |  createTime string
        |) PARTITIONED BY (ds STRING) STORED AS orc TBLPROPERTIES (
        | 'partition.time-extractor.timestamp-pattern'='ds',
        | 'sink.partition-commit.trigger'='process-time',
        | 'sink.partition-commit.delay'='1 min',
        | 'sink.partition-commit.policy.kind'='metastore,success-file'
        |)
        |""".stripMargin)
    sql(
      """
        |INSERT INTO TABLE hive.tmp.zfs_upload_record_topic_fire
        |SELECT id, name, age, createTime, '20230821' as ds
        |FROM default_catalog.default_database.source
        |""".stripMargin)
  }
}