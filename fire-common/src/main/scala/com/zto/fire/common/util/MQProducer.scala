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

package com.zto.fire.common.util

import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.conf.{FireFrameworkConf, FireKafkaConf, FireRocketMQConf}
import com.zto.fire.common.enu.{Datasource, JobType, Operation}
import com.zto.fire.predef._
import com.zto.fire.common.util.MQType.MQType
import com.zto.fire.common.util.ShutdownHookManager.DEFAULT_PRIORITY
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.rocketmq.client.producer.{DefaultMQProducer, SendCallback, SendResult}
import org.apache.rocketmq.common.message.Message
import org.apache.rocketmq.remoting.common.RemotingHelper

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 消息队列管理器：内置常用MQ的发送API，消息的key与value默认均为String类型
 * 注：考虑到spark和flink在实时场景下不需要额外的api消费mq的场景，故暂不提供消费api
 *
 * @author ChengLong 2022-07-29 10:02:48
 * @since 2.3.1
 */
class MQProducer(url: String, mqType: MQType = MQType.kafka,
                 otherConf: Map[String, String] = Map.empty) extends Logging {
  private lazy val maxRetries = FireFrameworkConf.exceptionTraceSendMQMaxRetries
  private lazy val sendTimeout = FireFrameworkConf.exceptionSendTimeout
  private var sendErrorCount = 0
  private lazy val isRelease = new AtomicBoolean(false)
  private var useKafka, useRocketmq = false
  private lazy val topics = new JHashSet[String]()

  /**
   * 添加血缘信息
   */
  @Internal
  private[this] def addLineage(topic: String): Unit = {
    if (!this.topics.contains(topic)) {
      this.topics.add(topic)
      val realUrl = if (MQType.rocketmq == mqType) FireRocketMQConf.rocketNameServer(url) else FireKafkaConf.kafkaBrokers(this.url)
      logInfo(
        s"""
           |--------> Sink ${mqType.toString} information. <--------
           |broker: $realUrl
           |topic: $topic
           |--------------------------------------------------------
           |""".stripMargin)
      LineageManager.addMQDatasource(Datasource.parse(mqType.toString), realUrl, topic, "", Operation.SINK)
    }
  }

  // kafka producer
  private lazy val kafkaProducer = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FireKafkaConf.kafkaBrokers(this.url))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, this.sendTimeout.toString)
    this.otherConf.foreach(prop => props.put(prop._1, prop._2))

    val producer = new KafkaProducer[String, String](props)
    this.useKafka = true
    producer
  }

  // rocketmq producer
  private lazy val rocketmqProducer = {
    val producer = new DefaultMQProducer("fire")
    producer.setNamesrvAddr(FireRocketMQConf.rocketNameServer(this.url))
    producer.setSendMsgTimeout(this.sendTimeout)
    producer.start()
    this.useRocketmq = true
    producer
  }

  /**
   * 释放producer资源
   */
  private[fire] def close: Unit = {
    if (this.isRelease.compareAndSet(false, true)) {
      if (this.useKafka) {
        this.kafkaProducer.flush()
        this.kafkaProducer.close()
      }

      if (this.useRocketmq) {
        this.rocketmqProducer.shutdown()
      }
    }
  }

  /**
   * 将消息发送到kafka
   *
   * @param record
   * 消息体
   */
  def sendKafkaRecord(record: MQRecord): Unit = {
    requireNonNull(record, record.topic, record.msg)("消息体参数不合法，请检查")

    if (this.sendErrorCount >= this.maxRetries) {
      this.kafkaProducer.close()
      logger.error(s"异常信息发送MQ重试${this.sendErrorCount}次仍失败，将退出异常信息发送！")
      return
    }

    val kafkaRecord = record.toKafka
    this.addLineage(record.topic)
    kafkaProducer.send(kafkaRecord, new Callback() {
      override def onCompletion(recordMetadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          sendErrorCount += 1
          logger.warn("Send msg to kafka failed!", exception)
        } else sendErrorCount = 0
      }
    })
  }

  /**
   * 发送消息到kafka
   *
   * @param topic
   * 主题名称
   * @param msg
   * 发送的消息
   */
  def sendKafka(topic: String, msg: String): Unit = {
    this.sendKafkaRecord(MQRecord(topic, msg))
  }

  /**
   * 将消息发送到RocketMQ
   *
   * @param record
   * 消息体
   * @param timeout
   * 发送超时时间
   */
  def sendRocketMQRecord(record: MQRecord, timeout: Long = 10000): Unit = {
    requireNonNull(record, record.topic, record.msg)("消息体参数不合法，请检查")

    if (this.sendErrorCount >= this.maxRetries) {
      this.rocketmqProducer.shutdown()
      return
    }

    val rocketRecord = record.toRocketMQ
    this.addLineage(record.topic)
    this.rocketmqProducer.send(rocketRecord, new SendCallback {
      override def onSuccess(sendResult: SendResult): Unit = {
        // do nothing
      }

      override def onException(exception: Throwable): Unit = {
        if (exception != null) {
          sendErrorCount += 1
          logger.warn("Send msg to rocketmq failed!", exception)
        } else sendErrorCount = 0
      }
    }, timeout)
  }

  /**
   * 发送消息到rocketmq
   *
   * @param topic
   * 主题名称
   * @param msg
   * 消息体
   * @param tags
   * tag
   * @param timeout
   * 发送超时时间
   */
  def sendRocketMQ(topic: String, msg: String, tags: String = "*", timeout: Long = 10000): Unit = {
    this.sendRocketMQRecord(MQRecord(topic, msg, null, null, tags, 0, true))
  }

  /**
   * 发送消息到指定的消息队列
   *
   * @param record
   * 消息体
   */
  def send(record: MQRecord): Unit = {
    this.mqType match {
      case MQType.rocketmq => this.sendRocketMQRecord(record)
      case _ => this.sendKafkaRecord(record)
    }
  }

  /**
   * 发送消息到指定的消息队列
   *
   * @param topic
   * 主题名称
   * @param msg
   * 消息体
   */
  def send(topic: String, msg: String): Unit = {
    this.send(MQRecord(topic, msg))
  }
}

object MQProducer {
  // 用于维护多个producer实例，避免重复创建
  private lazy val producerMap = new JConcurrentHashMap[String, MQProducer]()
  this.addHook()(this.release)

  /**
   * 释放所有使用了的producer资源，会被fire框架自动调用
   */
  @Internal
  private[fire] def release: Unit = {
    producerMap.foreach(t => t._2.close)
  }

  /**
   * 注册jvm退出前回调，在任务退出前完成消息的发出
   *
   * @param fun
   * 消息发送逻辑
   */
  @Internal
  private[fire] def addHook(priority: Int = ShutdownHookManager.LOW_PRIORITY)(fun: => Unit): Unit = {
    // 注册回调，在jvm退出前将所有异常发送到mq中
    ShutdownHookManager.addShutdownHook(priority)(() => {
      fun
    })
  }

  def apply(url: String, mqType: MQType = MQType.kafka,
            otherConf: Map[String, String] = Map.empty) = new MQProducer(url, mqType, otherConf)

  /**
   * 发送消息到指定的mq topic
   *
   * @param mqType
   * mq的类别：kafka/rocketmq
   * @param otherConf
   * 优化参数
   */
  def send(url: String, topic: String, msg: String,
           mqType: MQType = MQType.kafka, otherConf: Map[String, String] = Map.empty): Unit = {
    this.sendRecord(url, MQRecord(topic, msg), mqType, otherConf)
  }

  /**
   * 发送消息体到指定的mq topic
   *
   * @param mqType
   * mq的类别：kafka/rocketmq
   * @param otherConf
   * 优化参数
   */
  def sendRecord(url: String, record: MQRecord,
                 mqType: MQType = MQType.kafka, otherConf: Map[String, String] = Map.empty): Unit = {
    val producer = this.producerMap.mergeGet(url + ":" + record.topic)(new MQProducer(url, mqType, otherConf))
    producer.send(record)
  }

  /**
   * 将消息发送到kafka
   */
  def sendKafka(url: String, topic: String, msg: String, otherConf: Map[String, String] = Map.empty): Unit = this.send(url, topic, msg, MQType.kafka, otherConf)

  /**
   * 将消息发送到kafka
   */
  def sendKafkaRecord(url: String, record: MQRecord, otherConf: Map[String, String] = Map.empty): Unit = this.sendRecord(url, record, MQType.kafka, otherConf)


  /**
   * 将消息发送到rocketmq
   */
  def sendRocketMQ(url: String, topic: String, msg: String, otherConf: Map[String, String] = Map.empty): Unit = this.send(url, topic, msg, MQType.rocketmq, otherConf)

  /**
   * 将消息发送到rocketmq
   */
  def sendRocketMQRecord(url: String, record: MQRecord, otherConf: Map[String, String] = Map.empty): Unit = this.sendRecord(url, record, MQType.rocketmq, otherConf)
}

/**
 * 主流MQ产品枚举类
 */
object MQType extends Enumeration {
  type MQType = Value

  val kafka = Value("kafka")
  val rocketmq = Value("rocketmq")
  // 自动识别
  val auto = Value("auto")
}
