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

package com.zto.fire.flink.sink

import com.zto.fire._
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.conf.KeyNum
import com.zto.fire.common.util.{KafkaUtils, MQProducer, MQType}
import com.zto.fire.common.util.MQType.MQType
import org.apache.flink.configuration.Configuration

import scala.reflect.ClassTag

/**
 * Flink kafka sink组件，底层基于MQProducer
 *
 * @author ChengLong 2023-07-24 10:15:57
 * @since 2.3.8
 */
abstract class KafkaSink[IN, T <: MQRecord : ClassTag](params: Map[String, Object],
                                                       url: String, topic: String,
                                                       batch: Int = 100,
                                                       flushInterval: Long = 5000,
                                                       keyNum: Int = KeyNum._1) extends BaseSink[IN, T](batch, flushInterval) {

  private lazy val (finalBrokers, finalTopic, finalConf) = KafkaUtils.getConfByKeyNum(params, url, topic, keyNum)

  /**
   * 将数据sink到hbase
   * 该方法会被flush方法自动调用
   */
  override def sink(dataList: List[T]): Unit = {
    dataList.foreach(record => {
      if (isEmpty(record.topic)) record.topic = finalTopic
      MQProducer.sendRecord(finalBrokers, record, MQType.kafka, finalConf)
    })
  }
}
