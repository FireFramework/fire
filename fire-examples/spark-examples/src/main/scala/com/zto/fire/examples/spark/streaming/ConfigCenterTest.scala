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

package com.zto.fire.examples.spark.streaming

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.examples.bean.Student
import com.zto.fire.spark.BaseSparkStreaming
import com.zto.fire.spark.anno.StreamingDuration
import com.zto.fire.spark.util.SparkUtils

/**
 * 基于Fire进行Spark Streaming开发
 */
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire2
    |hive.cluster=test
    |fire.acc.timer.max.size=30
    |fire.acc.log.max.size=20
    |""")
@StreamingDuration(20) // spark streaming的批次时间
object ConfigCenterTest extends BaseSparkStreaming {

  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    val dstream = this.fire.createKafkaDirectStream()
    this.printConf

    dstream.foreachRDD(rdd => {
      rdd.map(t => {
        printConf
        JSONUtils.parseObject[Student](t.value())
      }).repartition(2)
    })

    this.fire.start
  }

  /**
   * 配置信息打印
   */
  def printConf: Unit = {
    println("================================")
    println("fire.thread.pool.size=" + this.conf.getInt("fire.thread.pool.size", -1))
    println("fire.thread.pool.schedule.size=" + this.conf.getInt("fire.thread.pool.schedule.size", -1))
    println("fire.acc.timer.max.size=" + this.conf.getInt("fire.acc.timer.max.size", -1))
    println("fire.acc.log.max.size=" + this.conf.getInt("fire.acc.log.max.size", -1))
    println("fire.jdbc.query.partitions=" + this.conf.getInt("fire.jdbc.query.partitions", -1))
    println("================================")
  }
}
