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
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.util.Collector

/**
 * 基于Fire进行Flink Streaming开发
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    | #注释信息
    |kafka.brokers.name = bigdata_test
    |flink.kafka.conf.restoredState.skip=true
    |kafka.topics = fire
    |kafka.group.id=fire
    |
    |kafka.brokers.name2 = bigdata_test
    |kafka.topics2 = fire2
    |kafka.group.id2=fire2
    |
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |flink.stream.checkpoint.interval=10000
    |flink.state.choose.disk.policy=round_robin
    |state.external.zookeeper.url=10.7.69.238:2181
    |fire.analysis.arthas.enable=true
    |fire.log.level.conf.org.apache.flink=warn
    |fire.analysis.arthas.tunnel_server.url=ws://10.7.69.32:7777/ws
    |fire.analysis.arthas.container.enable=false
    |""")
object Test extends BaseFlinkStreaming {

  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    println("----> " + System.getProperty("sun.net.inetaddr.ttl"))
    val dstream = this.fire.createKafkaDirectStream().filter(json => JSONUtils.isJson(json)).map(json => JSONUtils.parseObject[Student](json)).setParallelism(2)
    this.fire.createKafkaDirectStream(keyNum = 2).print()
    val value: KeyedStream[Student, JLong] = dstream.keyBy(t => t.getId)

    value.process(new KeyedProcessFunction[JLong, Student, String]() {

      override def processElement(value: Student, ctx: KeyedProcessFunction[_root_.com.zto.fire.JLong, Student, String]#Context, out: Collector[String]): Unit = {
        val state = this.getState[Long]("sum")
        state.update(state.value() + 1)
        println(s"当前key=${value.getId} sum=${state.value()}")
        println("----> " + System.getProperty("sun.net.inetaddr.ttl"))
        out.collect(value.getName)
      }

    }).print("name")
    this.fire.start
  }
}