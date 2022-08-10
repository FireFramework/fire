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

package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{Hive, Kafka}
import com.zto.fire.core.anno.lifecycle.{After, Before, Handle, Process, Step1, Step2}
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.{BaseSparkCore, BaseSparkStreaming}
import com.zto.fire.spark.anno.Streaming

/**
 * 基于Fire进行Spark Streaming开发
 *
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
// 60s一个批，最大同时执行2个streaming批次，开启反压机制、每个分区每秒最大消费100条消息
@Streaming(interval = 10, concurrent = 2, backpressure = true, maxRatePerPartition = 100)
// 配置消费的kafka信息
@Kafka(brokers = "bigdata_test", topics = "fire5", groupId = "fire")
@Hive("test")
object Test extends BaseSparkCore {

  override def process: Unit = {
    logInfo("process---")
  }

  @Process
  def proc: Unit = {
    logInfo("proc----")
  }

  @Handle
  def handle: Unit = {
    logInfo("handle----")
  }

  @Step1("计算订单量")
  def test: Unit = {
    logger.info("步骤1. process ................")
  }

  @Step2(value = "计算业务量", skipError = true)
  def test2: Unit = {
    logger.info("步骤2. process ................")
  }

  @Before
  def init1(): Unit = {
    logger.warn("Before------------------")
  }

  @After
  def destory1(): Unit = {
    logger.warn("After------------------")
  }
}
