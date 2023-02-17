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

package com.zto.fire.examples.flink.connector.rocketmq

import com.zto.fire.core.anno.lifecycle.{Step1, Step2, Step3}
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

/**
 * RocketMQ connector
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-01-18 17:24
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(parallelism = 8, interval = 30)
object RocketMQTest extends FlinkStreaming {

  @Step1("定义RocketMQ源表")
  def source: Unit = {
    sql("""
          |CREATE table source (
          |  id int,
          |  name string,
          |  age int,
          |  length double,
          |  data DECIMAL(10, 5)
          |) with (
          | 'connector' = 'rocketmq',
          | 'topic' = 'fire',
          | 'consumerGroup' = 'fire',
          | 'nameServerAddress' = 'rocketmq-server:9876'
          |)
          |""".stripMargin)
  }

  @Step2("定义目标表")
  def sink: Unit = {
    sql(
      """
        |CREATE TABLE sink WITH ('connector' = 'print')
        |LIKE source (EXCLUDING ALL)
        |""".stripMargin)
  }

  @Step3("数据sink")
  def insert: Unit = {
    sql("""
        |insert into sink select * from source
        |""".stripMargin)
  }
}
