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
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.anno.connector.{Kafka, Kafka2, Kafka3}
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming

/**
 * kafka json解析
 *
 * @author ChengLong 2019-6-26 16:52:58
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(interval = 10)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka2(brokers = "bigdata_test", topics = "fire2")
object KafkaSinkTest extends SparkStreaming {

  override def process: Unit = {
    // 从@Kafka注解配置的kafa消费数据：topic is fire
    val dstream = this.fire.createKafkaDirectStream()

    // 将数据写到@Kafka2注解配置的kafka中：topic is fire2
    dstream.foreachRDD(rdd => {
      rdd.sinkKafka(keyNum = 2)
    })
  }
}
