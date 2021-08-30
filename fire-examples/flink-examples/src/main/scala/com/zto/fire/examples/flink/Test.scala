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
import com.zto.fire.flink.BaseFlinkStreaming
import org.apache.flink.api.scala._

/**
 * 基于Fire进行Flink Streaming开发
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    | #注释信息
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire
    |kafka.brokers.name2 = bigdata_test
    |kafka.topics2 = fire2
    |kafka.group.id2=fire
    |hive.cluster=test
    |fire.thread.pool.size=7
    |fire.restful.max.thread=10
    |fire.jdbc.query.partitions=12
    |fire.hbase.scan.repartitions=100
    |fire.hbase.table.exists.cache.enable=false
    |""")
object Test extends BaseFlinkStreaming {

  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    val dstream = this.fire.createDirectStream()
    dstream.map(t => {
      println("message 1->" + t)
      t
    }).printToErr("kafka1->")

    val dstream2 = this.fire.createDirectStreamByJsonKeyValue(keyNum = 2)
    dstream2.map(t => {
      println("message 2->" + t)
      t
    }).printToErr("kafka2->")
    this.fire.start
  }
}