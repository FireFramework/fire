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
import com.zto.fire.common.anno.Config
import com.zto.fire.spark.BaseSparkStreaming
import com.zto.fire.spark.anno.StreamingDuration

/**
 * 基于Fire进行Spark Streaming开发
 */
@StreamingDuration(20) // spark streaming的批次时间
@Config(props = Array("kafka.brokers.name = bigdata_test", "kafka.topics = fire", "kafka.group.id=fire", "hive.cluster=test")) // 基于注解方式进行配置
object Test extends BaseSparkStreaming {
  /**
   * fire2.1不再需要main方法，逻辑直接放到process中
   */
  override def process: Unit = {
    println("-------->" + this.conf.getString("spark.hello"))
    this.args.foreach(println)
    val dstream = this.fire.createKafkaDirectStream()
    dstream.print
    this.fire.start
  }
}
