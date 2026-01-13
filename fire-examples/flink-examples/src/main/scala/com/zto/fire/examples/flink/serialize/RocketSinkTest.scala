package com.zto.fire.examples.flink.serialize

import com.zto.bigdata.integeration.util.ZtoSerializeUtil
import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.conf.{FireKafkaConf, FireRocketMQConf}
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.conf.{SerializeType, ZtoSerializeConf}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}

import java.util.Date

/**
 * @author zhanggougou 2025-12-08 10:09:19
 */
@Config(
  """
    |flink.rocket.brokers.name=testmq02ns1.test.ztosys.com:9876; testmq02ns2.test.ztosys.com:9876;testmq02ns3.test.ztosys.com:9876
    |flink.rocket.topics=fire_test_rocket_serialize
    |""")
@Streaming(interval = 300, unaligned = true, disableOperatorChaining = false, timeout = 180)
object RocketSinkTest extends FlinkStreaming {

  override def process(): Unit = {
    this.fire.createRandomIntStream(1).map(new RichMapFunction[Int,MQRecord] {

      override def open(parameters: Configuration): Unit = {
        ZtoSerializeUtil.initSerializerManager(FireRocketMQConf.rocketTopics(1)  ,null)
      }

      override def map(x: Int): MQRecord = {
        val outputData = ZtoSerializeUtil.serializerManager.outputData(ZtoSerializeUtil.classInfoMetadata.newInstance)
        outputData.setObject("id", x)
        outputData.setObject("name", x + "我是rocket")
        outputData.setObject("createTime", new Date())
        outputData.setObject("money", 1.6 + x)
        val bytes = ZtoSerializeUtil.serializerManager.serialize(outputData)
        val headMap =  Map((ZtoSerializeConf.SERIALIZER_HEAD_KEY,SerializeType.FURY.toString))
        MQRecord(null, null, headMap,bytes)
      }
    }).sinkRocketMQByte().name("testSink")
  }
}


