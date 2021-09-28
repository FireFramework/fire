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
import com.zto.fire.flink.ext.function.RuntimeContextExt
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
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
    |fire.print.limit=100
    |""")
object Test extends BaseFlinkStreaming {

  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    // val dstream = this.fire.createCollectionStream(Student.newStudentList())
    val dstream = this.fire.createKafkaDirectStream().filter(json => JSONUtils.isJson(json)).map(json => JSONUtils.parseObject[Student](json)).setParallelism(2)
    val value: KeyedStream[Student, JLong] = dstream.keyBy(t => t.getId)

    value.process(new KeyedProcessFunction[JLong, Student, String]() {

      override def processElement(value: Student, ctx: KeyedProcessFunction[_root_.com.zto.fire.JLong, Student, String]#Context, out: Collector[String]): Unit = {
        // 直接通过conf获取配置信息，无需复写open方法
        val broker = conf.getString("kafka.brokers.name")
        println("broker-->" + broker)
        val partitions = conf.getInt("fire.jdbc.query.partitions", 10)
        println("partitions-->" + partitions)
        // 直接获取runtimeContext变量
        println(this.runtimeContext.toString)
        // 直接通过this.getState获取状态，无需事先声明，fire框架会根据name值保证状态变量的单例
        val state = this.getState[String]("sum")
        state.update(state.value() + JSONUtils.toJSONString(value))

        out.collect(value.getName)
      }

    }).print("name")
    this.fire.start
  }
}