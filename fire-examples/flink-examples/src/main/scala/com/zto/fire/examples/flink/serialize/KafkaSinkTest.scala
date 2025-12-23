package com.zto.fire.examples.flink.serialize

import com.zto.bigdata.integeration.util.ZtoSerializeUtil
import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.conf.FireKafkaConf
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.conf.{SerializeType, ZtoSerializeConf}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}

import java.util.Date

/**
 * 使用自定义反序列化器发送kafka
 * 测试：10.7.114.156:9092,10.7.114.157:9092,10.7.114.158:9092
 * 生产：oggkafkanewb1.ztosys.com:9092,oggkafkanewb2.ztosys.com:9092,oggkafkanewb3.ztosys.com:9092
 * @author zhanggougou 2025-12-08 10:09:19
 */
@Config(
  """
    |kafka.brokers.name=10.7.114.156:9092,10.7.114.157:9092,10.7.114.158:9092
    |kafka.topics=fire_test_kafka_serialize
    |""")
@Streaming(interval = 300, unaligned = true, disableOperatorChaining = false, timeout = 180)
object KafkaSinkTest extends FlinkStreaming {

  override def process(): Unit = {
    this.fire.createRandomIntStream(1).map(new RichMapFunction[Int,MQRecord] {

      override def open(parameters: Configuration): Unit = {
        ZtoSerializeUtil.initSerializerManager(FireKafkaConf.kafkaTopics(1)  ,null)
      }

      override def map(x: Int): MQRecord = {
        val outputData = ZtoSerializeUtil.serializerManager.outputData(ZtoSerializeUtil.classInfoMetadata.newInstance)
        outputData.setObject("id", x)
        outputData.setObject("name", x + "我")
        outputData.setObject("createTime", new Date())
        outputData.setObject("money", 1.6 + x)
        val bytes = ZtoSerializeUtil.serializerManager.serialize(outputData)
        val headers = new RecordHeaders()
        headers.add(new RecordHeader(ZtoSerializeConf.SERIALIZER_HEAD_KEY, SerializeType.FURY.toString.getBytes()))
        MQRecord(null, null, headers,bytes)
      }
    }).sinkKafkaByte().name("testSink")
  }
}


