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
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming


@Streaming(interval = 20, disableOperatorChaining = true, parallelism = 2)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object Test extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    // 基于JavaBean创建json流（根据JavaBean序列化为json），JavaBean必须实现Generator接口
    val jsonStream = this.fire.createJSONStream[Student](1)
    jsonStream.print

    // 创建uuid流
    val uuidStream = this.fire.createUUIDStream(qps = 1)
    uuidStream.print

    // 创建Int型随机数流
    val intStream = this.fire.createRandomIntStream(1)
    intStream.print

    // 创建Long型随机数流
    val longStream = this.fire.createRandomLongStream(1)
    longStream.print

    // 创建Double型随机数流
    val doubleStream = this.fire.createRandomDoubleStream(1)
    doubleStream.print

    // 创建Float型随机数流
    val flaotStream = this.fire.createRandomFloatStream(1)
    flaotStream.print
  }
}