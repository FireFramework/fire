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
import com.zto.fire.common.util.ReflectionUtils
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

@Config(
  """
    |fire.lineage.debug.enable=false
    |fire.debug.class.code.resource=com.zto.fire.common.util.PropUtils,com.zto.fire.examples.flink.Test
    |""")
@Streaming(interval = 20, disableOperatorChaining = true, parallelism = 2)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka2(brokers = "bigdata_test", topics = "fire", groupId = "fire2")
object Test extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    println("主类：PropUtils=" + Class.forName("com.zto.fire.common.util.PropUtils").getName)
    println("PropUtils.classLoader=" + Class.forName("com.zto.fire.common.util.PropUtils").getClassLoader.toString)
    println("Test.classLoader=" + Thread.currentThread().getContextClassLoader.toString)
    val stream = this.fire.createKafkaDirectStream()
    stream.print()
  }
}