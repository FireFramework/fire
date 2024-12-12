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

package com.zto.fire.examples.flink.sql

import com.zto.fire.core.anno.lifecycle.{Step1, Step2}
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
@Streaming(parallelism = 2, interval = 30, disableOperatorChaining = true)
object RocketMQConnectorTest extends FlinkStreaming {

  @Step1("测试raw format")
  def raw: Unit = {
    sql("""
          |CREATE table rawSource (
          |  name STRING
          |) with (
          | 'connector'='fire-rocketmq',
          | 'format'='raw',
          | 'rocket.brokers.name'='bigdata_test',
          | 'rocket.topics'='fire-raw',
          | 'rocket.group.id'='fire-raw',
          | 'rocket.consumer.tag'='*'
          |)
          |""".stripMargin)

    sql(
      """
        |CREATE table rawSink (
        |  name STRING
        |) with (
        | 'connector'='fire-rocketmq',
        | 'format'='json',
        | 'rocket.brokers.name'='bigdata_test',
        | 'rocket.topics'='fire-raw2',
        | 'rocket.consumer.tag'='*',
        | 'rocket.sink.parallelism'='1'
        |)
        |""".stripMargin)

    sql("""
          |insert into rawSink select * from rawSource
          |""".stripMargin)
  }

  @Step2("测试json format")
  def json: Unit = {
    sql("""
          |CREATE table jsonSource (
          |  id int,
          |  name string,
          |  age int,
          |  length double,
          |  data DECIMAL(10, 5)
          |) with (
          | 'connector'='fire-rocketmq',
          | 'format'='json',
          | 'rocket.brokers.name'='bigdata_test',
          | 'rocket.topics'='fire',
          | 'rocket.group.id'='fire',
          | 'rocket.consumer.tag'='*'
          |)
          |""".stripMargin)
    sql(
      """
        |CREATE table jsonSink (
        |  id int,
        |  name string,
        |  age int,
        |  length double,
        |  data DECIMAL(10, 5)
        |) with (
        | 'connector'='fire-rocketmq',
        | 'format'='json',
        | 'rocket.brokers.name'='bigdata_test',
        | 'rocket.topics'='fire2',
        | 'rocket.consumer.tag'='*',
        | 'rocket.sink.parallelism'='1'
        |)
        |""".stripMargin)

    sql("""
          |insert into jsonSink select * from jsonSource
          |""".stripMargin)
  }
}
