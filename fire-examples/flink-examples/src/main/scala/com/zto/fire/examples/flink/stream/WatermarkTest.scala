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

package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala._

/**
 * 基于fire生成watermark的使用demo
 *
 * @author ChengLong 2020-4-13 15:58:38
 */
@Config(
  """
    |flink.stream.time.characteristic    =       EventTime
    |flink.default.parallelism           =       2
    |""")
@Hive("fat")
// 相当于：env.getConfig.setAutoWatermarkInterval(100L)
@Streaming(interval = 10, watermarkInterval = 100)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object WatermarkTest extends FlinkStreaming {

  override def process(): Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    dstream
      .map(t => JSONUtils.parseObject[Student](t))
      // 为可能乱序的流生成watermark
      // 第一个参数是一个函数，定义如何从消息中提前时间戳
      // 第二个参数表示允许乱序的最大时间，默认3s
      .assignOutOfOrdernessWatermarks(t => t.getId)
      // .assignOutOfOrdernessWatermarks(t => t.getId, Duration.ofSeconds(1))
      .print()
  }
}
