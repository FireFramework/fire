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
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.util.{JSONUtils, ThreadUtils}
import com.zto.fire.core.anno.connector.{Kafka, Kafka5, RocketMQ, RocketMQ3}
import com.zto.fire.spark.SparkStreaming
import com.zto.fire.spark.anno.Streaming
import com.zto.fire.spark.sync.SparkLineageAccumulatorManager

import java.util.concurrent.TimeUnit

/**
 * kafka json解析
 *
 * @author ChengLong 2019-6-26 16:52:58
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Streaming(interval = 10)
@RocketMQ(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka5(brokers = "bigdata_test", topics = "mq_test", groupId = "fire")
@RocketMQ3(brokers = "bigdata_test", topics = "mq_test", groupId = "fire")
object SinkKafkaRocketMQTest extends SparkStreaming {

  override def process: Unit = {
    // 从@Kafka注解配置的kafa消费数据：topic is fire
    val dstream = this.fire.createRocketMqPullStream()

    // 将数据写到@Kafka2注解配置的kafka中：topic is mq_test
    dstream.foreachRDD(rdd => {
      // MQRecord("hello", partition = 0)可以指定分区、key、tag、topic等多个参数，通过构造方法指定即可
      // 如：val recordRDD = rdd.map(t => MQRecord("topic", "msg", 10, "key", "tag"))
      val recordRDD = rdd.repartition(10).map(t => MQRecord("hello", partition = 0))
      // 将数据发送到@Kafka对应的topic：mq_test
      // recordRDD.sinkKafka()
      // 将数据发送到@Kafka5对应的topic：mq_test
      recordRDD.sinkKafka(keyNum = 5)
      // 将数据发送到@RocketMQ3配置的topic：mq_test
      recordRDD.sinkRocketMQ(keyNum = 3)
    })

    ThreadUtils.scheduleAtFixedRate({
      println(s"累加器值：" + JSONUtils.toJSONString(SparkLineageAccumulatorManager.getValue))
    }, 0, 10, TimeUnit.SECONDS)
  }
}
