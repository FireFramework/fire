package com.zto.fire.examples.flink.connector.kafka

import com.zto.fire._
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.core.anno.connector.{Kafka, Kafka2, Kafka5, RocketMQ3}
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala._

@Streaming(interval = 30, disableOperatorChaining = true)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka2(brokers = "bigdata_test", topics = "fire2")
@Kafka5(brokers = "bigdata_test", topics = "mq_test")
@RocketMQ3(brokers = "bigdata_test", topics = "mq_test", sinkBatch = 10, sinkFlushInterval = 3000, tag = "*")
object KafkaSinkTest extends FlinkStreaming {

  @Process
  def kafkaSink: Unit = {
    // 1. @Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")消费数据
    val dstream = this.fire.createKafkaDirectStream()

    // 2. keyNum=2对应注解@Kafka2(brokers = "bigdata_test", topics = "fire2")
    // 也就是将数据发送到bigdata_test对应的kafka，topic是fire2
    // dstream.sinkKafkaString(keyNum = 2).uname("sinkKafkaString")

    // 将数据发送到@Kafka5对应的topic：mq_test
    dstream.map(t => MQRecord(t)).sinkKafka(keyNum = 5, batch = 10, flushInterval = 2000).uname("sinkKafka")

    // 将数据发送到@RocketMQ3配置的topic以及partition 2这个分区中：mq_test
    dstream.map(t => MQRecord(t, key = "topic", partition = 2)).sinkRocketMQ(keyNum = 3).uname("sinkRocketMQ")
  }
}
