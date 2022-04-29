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
import com.zto.fire.common.enu.JdbcDriver
import com.zto.fire.core.anno._
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.anno.Checkpoint
import org.apache.commons.lang3.StringUtils

/**
 * 基于Fire进行Flink Streaming开发
 */
@Config(
  """
    |# 是否覆盖状态中的offset（请谨慎配置，用于kafka集群迁移等不正常状况的运维）
    |flink.kafka.force.overwrite.stateOffset.enable=true
    |# 是否在开启checkpoint的情况下强制开启周期性offset提交
    |flink.kafka.force.autoCommit.enable=true
    |# 周期性提交offset的时间间隔（ms）
    |flink.kafka.force.autoCommit.Interval=10000
    |
    | #注释信息
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire4
    |
    |kafka.brokers.name2 = bigdata_test
    |kafka.topics2 = fire2
    |kafka.group.id2=fire4
    |kafka.starting.offsets2=earliest
    |
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |flink.stream.checkpoint.interval=60000
    |flink.state.choose.disk.policy=round_robin
    |state.external.zookeeper.url=10.7.69.238:2181
    |fire.analysis.arthas.enable=false
    |fire.log.level.conf.org.apache.flink=warn
    |fire.analysis.arthas.tunnel_server.url=ws://10.7.69.32:7777/ws
    |fire.analysis.arthas.container.enable=false
    |""")
@HBase("batch")
@Hive("test")
@Checkpoint(interval = 100, unaligned = true)
@Kafka(brokers = "kafka", topics = "fire", groupId = "fire")
object Test extends BaseFlinkStreaming {

  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    println(this.conf.getString("hive.cluster"))
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print("fire1==> ")

    val dstream2 = this.fire.createKafkaDirectStream(keyNum = 2)
    dstream2.print("fire2--> ")

    this.fire.start
  }
}