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

package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.util.MQProducer
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * 使用自定义反序列化器消费kafka
 *
 * @author ChengLong 2024-05-13 10:09:19
 */
@Config(
  """
    |fire.lineage.debug.enable=false
    |fire.debug.class.code.resource=org.apache.flink.table.api.internal.TableEnvironmentImpl,com.zto.fire.examples.flink.Test,org.apache.hadoop.hbase.client.ConnectionManager
    |""")
@Streaming(interval = 20, disableOperatorChaining = true, parallelism = 2)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object KafkaDeserializationTest extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    // 使用自定义反序列化器
    val dstream = this.fire.createDirectStreamBySchema[Any](deserializer = new KeyValueDeserializationSchema, keyNum = 1)
    dstream.print()
  }
}

/**
 * 自定义kafka反序列化器
 *
 * @author ChengLong
 * @Date 2024/5/13 09:33
 * @version 2.3.
 */
class KeyValueDeserializationSchema extends KafkaDeserializationSchema[(String, String)] {

  override def isEndOfStream(t: (String, String)): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
    // 解析消息中的key与value
    val key = if (consumerRecord.key() == null) "" else new String(consumerRecord.key())
    (key, new String(consumerRecord.value()))
  }

  override def getProducedType: TypeInformation[(String, String)] = TypeExtractor.getForClass(classOf[(String, String)])
}
