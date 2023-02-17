package com.zto.fire.examples.flink.connector.kafka

import com.zto.fire._
import org.apache.flink.api.scala._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{Kafka, Kafka2}
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.examples.bean.Student
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

@Streaming(30)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
@Kafka2(brokers = "bigdata_test", topics = "fire2")
object KafkaSinkTest extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    // 1. @Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")消费数据
    val dstream = this.fire.createKafkaDirectStream()

    // 2. keyNum=2对应注解@Kafka2(brokers = "bigdata_test", topics = "fire2")
    // 也就是将数据发送到bigdata_test对应的kafka，topic是fire2
    dstream.sinkKafka(keyNum = 2)
  }
}
