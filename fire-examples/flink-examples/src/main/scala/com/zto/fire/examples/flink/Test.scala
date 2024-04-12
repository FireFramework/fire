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

import org.apache.flink.api.scala._
import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema


@Config(
  """
    |fire.lineage.debug.enable=false
    |""")
@Streaming(interval = 20, disableOperatorChaining = true, parallelism = 2)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka2(brokers = "bigdata_test", topics = "fire", groupId = "fire2")
object Test extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    val stream = this.fire.createKafkaDirectStream()
    stream.print()

    val dstream = this.fire.createDirectStreamBySchema[ObjectNode](deserializer = new JSONKeyValueDeserializationSchema(true), keyNum = 1)
    dstream.map(t => {
      t
    }).print("2->")
  }
}