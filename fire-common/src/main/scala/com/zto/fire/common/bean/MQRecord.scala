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
import org.apache.kafka.common.header.internals.RecordHeaders
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
  var tag: String = null
  var flag: Int = 0
  var waitStoreMsgOK: Boolean = true
  var kafkaHeaders: RecordHeaders = null
  // mq自定义头部信息，message的property属性
  var mqHeaders: Map[String, String] = Map()
  //特殊场景需要value发送byte
  var msgBytes: Array[Byte] = null

  def this(topic: String, msg: String, partition: JInt, key: String, tag: String, flag: Int, waitStoreMsgOK: Boolean, mqHeaders: Map[String, String], msgBytes: Array[Byte]) = {
    this(topic, msg)
    this.partition = partition
    this.key = key
    this.tag = tag
    this.flag = flag
    this.waitStoreMsgOK = waitStoreMsgOK
    this.mqHeaders = mqHeaders
    this.msgBytes = msgBytes
  }

  def this(topic: String, msg: String, partition: JInt, key: String, tag: String, flag: Int, waitStoreMsgOK: Boolean,kafkaHeaders:RecordHeaders,msgBytes:Array[Byte]) = {
    this(topic, msg)
    this.partition = partition
    this.key = key
    this.tag = tag
    this.flag = flag
    this.waitStoreMsgOK = waitStoreMsgOK
    this.kafkaHeaders = kafkaHeaders
    this.msgBytes = msgBytes
  }

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
    this(null, msg, partition, key, null, 0, true)
  }

  def this(msg: String, partition: JInt) = {
    this(null, msg, partition, null, null, 0, true)
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
    this(null, msg, null, null, null, 0, true)
  }

  /**
   * 转为RocketMQ消息体
   */
  def toRocketMQ: Message = {
    new Message(topic, if (isEmpty(tag)) "*" else tag, key, flag, msg.getBytes(RemotingHelper.DEFAULT_CHARSET), waitStoreMsgOK)
  }

  /**
   * 转为RocketMQ 原生bbyte消息体
   */
  def toRocketMQBytes: Message = {
    new Message(topic, if (isEmpty(tag)) "*" else tag, key, flag, msgBytes, waitStoreMsgOK)
  }

  def toRocketMQByFlag(sendBytes: Boolean): Message = {
    if(sendBytes){
      this.toRocketMQBytes
    } else{
      this.toRocketMQ
    }
  }


  /**
   * 转为Kafka消息体
   */
  def toKafka: ProducerRecord[String, String] = {
    new ProducerRecord[String, String](topic, partition, key, msg, kafkaHeaders)
  }

  /**
   * 转为Kafka byte消息体
   */
  def toKafkaBytes: ProducerRecord[String, Array[Byte]] = {
    new ProducerRecord[String, Array[Byte]](topic, partition, key, msgBytes, kafkaHeaders)
  }

  def toKafkaByFlag(sendBytes: Boolean): ProducerRecord[_, _] = {
    if (sendBytes) {
      this.toKafkaBytes
    } else {
      this.toKafka
    }
  }

}

object MQRecord {
  def apply(topic: JString, msg: String, partition: JInt, key: String, tags: JString, flag: Int, waitStoreMsgOK: Boolean): MQRecord = new MQRecord(topic, msg, partition, key, tags, flag, waitStoreMsgOK)

  def apply(msg: String, partition: JInt, key: String, tags: JString, flag: Int, waitStoreMsgOK: Boolean): MQRecord = new MQRecord(null, msg, partition, key, tags, flag, waitStoreMsgOK)

  def apply(msg: String, partition: JInt, key: String, tags: JString, flag: Int): MQRecord = new MQRecord(null, msg, partition, key, tags, flag, true)

  def apply(key: String,tags: JString, headers: RecordHeaders,msgBytes:Array[Byte]): MQRecord = new MQRecord(null, null, null, key, tags, 0, true,headers,msgBytes)

  def apply(key: String,tags: JString, mqHeaders: Map[String, String],msgBytes:Array[Byte]): MQRecord = new MQRecord(null, null, null, key, tags, 0, true,mqHeaders,msgBytes)

  def apply(msg: String, partition: JInt, key: String, tags: JString, headers: RecordHeaders,msgBytes:Array[Byte]): MQRecord = new MQRecord(null, msg, partition, key, tags, 0, true,headers,msgBytes)

  def apply(msg: String, partition: JInt, key: String, tags: JString): MQRecord = new MQRecord(null, msg, partition, key, tags, 0, true)

  def apply(msg: String, partition: JInt, key: String): MQRecord = new MQRecord(null, msg, partition, key, null, 0, true)

  def apply(msg: String, partition: JInt): MQRecord = new MQRecord(null, msg, partition, null, null, 0, true)

  def apply(msg: String): MQRecord = new MQRecord(null, msg, null, null, null, 0, true)

  def apply(topic: String, msg: String): MQRecord = new MQRecord(topic, msg, null, null, null, 0, true)

  def apply(topic: String, msg: String, partition: JInt): MQRecord = new MQRecord(topic, msg, partition, null, null, 0, true)

  def apply(topic: String, msg: String, partition: JInt, key: String): MQRecord = new MQRecord(topic, msg, partition, key, null, 0, true)

  def apply(topic: String, msg: String, partition: JInt, key: String, tag: JString): MQRecord = new MQRecord(topic, msg, partition, key, tag, 0, true)

  def apply(topic: String, msg: String, partition: JInt, key: String, tag: JString, flag: Int): MQRecord = new MQRecord(topic, msg, partition, key, tag, flag, true)
}