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

package com.zto.fire.examples.spark.thread

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.{DateFormatUtils, ThreadUtils}
import com.zto.fire.spark.BaseSparkStreaming

/**
  * 在driver中启用线程池的示例
  * 1. 开启子线程执行一个任务
  * 2. 开启子线程执行周期性任务
  */
@Config(
  """
    |spark.log.level                    =       INFO
    |# 非必须配置项：默认就是这个地址
    |spark.kafka.brokers.name           =       zms
    |# 必须配置项：kafka的topic列表，以逗号分隔
    |spark.kafka.topics                 =       aries_binlog_order
    |# 非必须配置项：默认为appName
    |spark.kafka.group.id               =       OrderDetailMainCommon
    |
    |# ------------------- < hbase 配置 > ------------------- #
    |# 用于区分不同的hbase集群: batch/streaming/old
    |spark.hbase.cluster                =       streaming
    |
    |# spark的参数可以直接写在下面，都会被加载，覆盖程序中默认的配置信息
    |spark.speculation                  =       false
    |spark.streaming.concurrentJobs     =       1
    |""")
object ThreadTest extends BaseSparkStreaming {

  override def main(args: Array[String]): Unit = {
    // 第二个参数为true表示开启checkPoint机制
    this.init(10L, false)
  }

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    // 第一次执行时延迟两分钟，每隔1分钟执行一次showSchema函数
    ThreadUtils.schedule(this.showSchema, 1, 1)
    // 以子线程方式执行print方法中的逻辑
    ThreadUtils.run(this.print)

    val dstream = this.fire.createKafkaDirectStream()
    dstream.foreachRDD(rdd => {
      println("count--> " + rdd.count())
    })

    this.fire.start
  }

  /**
    * 以子线程方式执行一次
    */
  def print: Unit = {
    println("==========子线程执行===========")
  }

  /**
    * 查看表结构信息
    */
  def showSchema: Unit = {
    println(s"${DateFormatUtils.formatCurrentDateTime()}--------------> atFixRate <----------------")
    this.fire.sql("use tmp")
    this.fire.sql("show tables").show(false)
  }
}
