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
import com.zto.fire.spark.BaseSparkStreaming
import com.zto.fire.spark.anno.StreamingDuration
import com.zto.fire.spark.util.SparkUtils

/**
 * 基于Fire进行Spark Streaming开发
 */
@StreamingDuration(20) // spark streaming的批次时间
@Config(
  """
    |# 直接从配置文件中拷贝过来即可
    |kafka.brokers.name = bigdata_test
    |kafka.topics = fire
    |kafka.group.id=fire
    |hive.cluster=test
    |fire.thread.pool.size=7
    |fire.restful.max.thread=10
    |fire.jdbc.query.partitions=12
    |fire.hbase.scan.repartitions=100
    |fire.hbase.table.exists.cache.enable=false
    |""")
object ConfigCenterTest extends BaseSparkStreaming {


  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    this.printConf

    val dstream = this.fire.createKafkaDirectStream()
    dstream.foreachRDD(rdd => {
      rdd.repartition(5).foreachPartition(it => {
        this.printConf
      })
    })
    this.fire.start
  }

  def printConf: Unit = {
    println(SparkUtils.getExecutorId + " fire.thread.pool.size-------->" + this.conf.getString("fire.thread.pool.size"))  // 10
    println(SparkUtils.getExecutorId + " fire.restful.max.thread-------->" + this.conf.getString("fire.restful.max.thread"))  // 12
    println(SparkUtils.getExecutorId + " fire.jdbc.query.partitions-------->" + this.conf.getString("fire.jdbc.query.partitions"))  // 11
    println(SparkUtils.getExecutorId + " fire.hbase.batch.size-------->" + this.conf.getString("fire.hbase.batch.size"))  // 100
    println(SparkUtils.getExecutorId + " fire.hbase.scan.repartitions-------->" + this.conf.getString("fire.hbase.scan.repartitions"))  // 110
    println(SparkUtils.getExecutorId + " fire.hbase.table.exists.cache.enable-------->" + this.conf.getBoolean("fire.hbase.table.exists.cache.enable", true))  // false
    println(SparkUtils.getExecutorId + " fire.hbase.table.exists.cache.period-------->" + this.conf.getInt("fire.hbase.table.exists.cache.period", 500))  // 600
  }
}
