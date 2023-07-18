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

package com.zto.fire.common.bean

import com.zto.fire.predef._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.rocketmq.common.message.Message
import org.apache.rocketmq.remoting.common.RemotingHelper

/**
 * 消息队列消息体
 *
 * @author ChengLong 2023-07-17 16:39:08
 * @since 2.3.8
 */
class MQRecord(var topic: String, var msg: String) {
  var partition: JInt = null
  var key: String = null
  var tag: String = "*"
  var flag: Int = 0
  var waitStoreMsgOK: Boolean = true

  def this(topic: String, msg: String, partition: JInt, key: String, tag: String, flag: Int, waitStoreMsgOK: Boolean) = {
    this(topic, msg)
    this.partition = partition
    this.key = key
    this.tag = tag
    this.flag = flag
    this.waitStoreMsgOK = waitStoreMsgOK
  }

  def this(msg: String, partition: JInt, key: String, tag: String, flag: Int, waitStoreMsgOK: Boolean) = {
    this(null, msg, partition, key, tag, flag, waitStoreMsgOK)
  }

  def this(msg: String, partition: JInt, key: String) = {
    this(null, msg, partition, key, "*", 0, true)
  }

  def this(msg: String, partition: JInt) = {
    this(null, msg, partition, null, "*", 0, true)
  }

  def this(msg: String, key: String, tag: String, flag: Int, waitStoreMsgOK: Boolean) = {
    this(null, msg, null, key, tag, flag, waitStoreMsgOK)
  }

  def this(msg: String, key: String, tag: String) = {
    this(null, msg, null, key, tag, 0, true)
  }

  def this(msg: String, key: String, tag: String, flag: Int) = {
    this(null, msg, null, key, tag, flag, true)
  }

  def this(msg: String) = {
    this(null, msg, null, null, "*", 0, true)
  }

  /**
   * 转为RocketMQ消息体
   */
  def toRocketMQ: Message = {
    new Message(topic, tag, key, flag, msg.getBytes(RemotingHelper.DEFAULT_CHARSET), waitStoreMsgOK)
  }

  /**
   * 转为Kafka消息体
   */
  def toKafka: ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, partition, key, msg)
  }
}

object MQRecord {
  def apply(topic: JString, msg: String, partition: JInt, key: String, tags: JString, flag: Int, waitStoreMsgOK: Boolean): MQRecord = new MQRecord(topic, msg, partition, key, tags, flag, waitStoreMsgOK)

  def apply(msg: String, partition: JInt, key: String, tags: JString, flag: Int, waitStoreMsgOK: Boolean): MQRecord = new MQRecord(null, msg, partition, key, tags, flag, waitStoreMsgOK)

  def apply(msg: String, partition: JInt, key: String, tags: JString, flag: Int): MQRecord = new MQRecord(null, msg, partition, key, tags, flag, true)

  def apply(msg: String, partition: JInt, key: String, tags: JString): MQRecord = new MQRecord(null, msg, partition, key, tags, 0, true)

  def apply(msg: String, partition: JInt, key: String): MQRecord = new MQRecord(null, msg, partition, key, "*", 0, true)

  def apply(msg: String, partition: JInt): MQRecord = new MQRecord(null, msg, partition, null, "*", 0, true)

  def apply(msg: String): MQRecord = new MQRecord(null, msg, null, null, "*", 0, true)

  def apply(topic: String, msg: String): MQRecord = new MQRecord(topic, msg, null, null, "*", 0, true)

  def apply(topic: String, msg: String, partition: JInt): MQRecord = new MQRecord(topic, msg, partition, null, "*", 0, true)

  def apply(topic: String, msg: String, partition: JInt, key: String): MQRecord = new MQRecord(topic, msg, partition, key, "*", 0, true)

  def apply(topic: String, msg: String, partition: JInt, key: String, tag: JString): MQRecord = new MQRecord(topic, msg, partition, key, tag, 0, true)

  def apply(topic: String, msg: String, partition: JInt, key: String, tag: JString, flag: Int): MQRecord = new MQRecord(topic, msg, partition, key, tag, flag, true)
}