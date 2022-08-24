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
import org.apache.flink.api.scala._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

@Config(
  """
    |spark.streaming.batch.duration=hello
    |""")
@Streaming(interval = 100, unaligned = true, parallelism = 30) // 100s做一次checkpoint，开启非对齐checkpoint
@Kafka(brokers = "ip:9091,ip2:9092", topics = "fire", groupId = "fire")
@Kafka2(brokers = "ip:9091,ip2:9092", topics = "fire2", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object Test extends FlinkStreaming {

  /**
   * 业务逻辑代码，会被fire自动调用
   */
  @Process
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream().map(t => JSONUtils.parseObject[Student](t))
    this.fire.createKafkaDirectStream(keyNum = 2)
    val list = new JHashMap[String, String]()
    dstream.map(t => {
      val id = t.getId / 1
      this.conf.getString("spark.streaming.batch.duration")
      t
    }).createOrReplaceTempView("t_student")
  }
}