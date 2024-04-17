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

package com.zto.fire.spark.util

import com.zto.fire._
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.{ConsumerOffsetInfo, JobConsumerInfo}
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.core.util.ConsumerOffsetManager
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.rocketmq.spark.{TopicQueueId, OffsetRange => RocketMQOffsetRange}

/**
 * 实时计算引擎消费位点管理器
 *
 * @author ChengLong
 * @Date 2024/4/17 13:52
 * @version 2.4.5
 */
@Internal
private[fire] object SparkConsumerOffsetManager extends ConsumerOffsetManager {

  /**
   * 将Spark Streaming每个批次的消费位点信息转为JobConsumerInfo类型
   * @param offsetRanges
   * Spark Streaming各批次消费位点信息
   */
  private[this] def toConsumerInfo(offsetRanges: Array[OffsetRange]): Option[JobConsumerInfo] = {
    if (offsetRanges == null || offsetRanges.isEmpty) return None

    val offsetSetInfo = new JHashSet[ConsumerOffsetInfo]()
    offsetRanges.foreach(range => {
      if (range.untilOffset > range.fromOffset) {
        offsetSetInfo.add(new ConsumerOffsetInfo(range.topic, range.partition, range.untilOffset))
      }
    })

    if (offsetSetInfo.isEmpty) return None

    Some(new JobConsumerInfo(offsetSetInfo))
  }

  /**
   * 将Spark Streaming每个批次的消费位点信息转为JobConsumerInfo类型
   *
   * @param offsetRanges
   * Spark Streaming各批次消费位点信息
   */
  private[this] def toConsumerInfo(offsetRanges: JMap[TopicQueueId, Array[RocketMQOffsetRange]]): Option[JobConsumerInfo] = {
    if (offsetRanges == null || offsetRanges.isEmpty) return None

    val offsetSetInfo = new JHashSet[ConsumerOffsetInfo]()
    offsetRanges.foreach(offsetRange => {
      offsetRange._2.foreach(range => {
        if (range.untilOffset > range.fromOffset) {
          offsetSetInfo.add(new ConsumerOffsetInfo(range.topic, range.brokerName, range.queueId, range.untilOffset))
        }
      })
    })

    if (offsetSetInfo.isEmpty) return None

    Some(new JobConsumerInfo(offsetSetInfo))
  }

  /**
   * 将kafka消费位点信息转换并发送到消息队列
   *
   * @param offsetRanges
   * Spark kafka offsetRanges
   */
  def post(offsetRanges: Array[OffsetRange]): Unit = {
    if (!FireFrameworkConf.consumerOffsetExportEnable) return

    tryWithLog {
      val jobConsumerInfo = this.toConsumerInfo(offsetRanges)
      if (jobConsumerInfo.isDefined && noEmpty(jobConsumerInfo.get.getOffsets)) {
        this.post(jobConsumerInfo.get)
      }
    } (this.logger, catchLog = "采集Kafka消费位点信息失败，请检查", isThrow = false, hook = false)
  }

  /**
   * 将RocketMQ消费位点信息转换并发送到消息队列
   *
   * @param offsetRanges
   * Spark rocketmq offsetRanges
   */
  def post(offsetRanges: JMap[TopicQueueId, Array[RocketMQOffsetRange]]): Unit = {
    if (!FireFrameworkConf.consumerOffsetExportEnable) return

    tryWithLog {
      val jobConsumerInfo = this.toConsumerInfo(offsetRanges)
      if (jobConsumerInfo.isDefined && noEmpty(jobConsumerInfo.get.getOffsets)) {
        this.post(jobConsumerInfo.get)
      }
    } (this.logger, catchLog = "采集RocketMQ消费位点信息失败，请检查", isThrow = false, hook = false)
  }
}
