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
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.ext.function.FireMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.KeyedStream

/**
 * 基于Fire进行Flink Streaming开发
 */
@Config(props = Array("kafka.brokers.name = bigdata_test", "kafka.topics = fire", "kafka.group.id=fire", "fire.print.limit=100", "flink.state.log.threshold=1")) // 基于注解方式进行配置
object Test extends BaseFlinkStreaming {

  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    // val dstream = this.fire.createCollectionStream(Student.newStudentList())
    val dstream = this.fire.createKafkaDirectStream().filter(json => JSONUtils.isJson(json)).map(json => JSONUtils.parseObject[Student](json)).setParallelism(2)
    val value: KeyedStream[Student, JLong] = dstream.keyBy(t => t.getId)

    value.map(new FireMapFunction[Student, String] {

      override def map(t: Student): String = {
        // 直接通过conf获取配置信息，无需复写open方法
        val broker = conf.getString("kafka.brokers.name")
        println("broker-->" + broker)
        val partitions = conf.getInt("fire.jdbc.query.partitions", 10)
        println("partitions-->" + partitions)

        val valueState = this.getState[String]("value_state")
        valueState.update(valueState.value() + t.getName)
        println("状态值：" + valueState.value())
        t.getName
      }
    }).print("name")

    this.fire.start
  }
}