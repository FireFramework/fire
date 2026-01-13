
package com.zto.fire.examples.flink.serialize

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala.createTypeInformation


/**
 * 使用自定义反序列化器消费kafka
 * 测试：10.7.114.156:9092,10.7.114.157:9092,10.7.114.158:9092
 * 生产：oggkafkanewb1.ztosys.com:9092,oggkafkanewb2.ztosys.com:9092,oggkafkanewb3.ztosys.com:9092
 * @author zhanggougou 2025-12-08 10:09:19
 */
@Config(
  """
    |kafka.brokers.name=10.7.114.156:9092,10.7.114.157:9092,10.7.114.158:9092
    |kafka.topics=fire_test_kafka_serialize
    |kafka.group.id=fire_test_kafka_serialize_consumer
    |kafka.starting.offsets=earliest
    |""")
@Streaming(interval = 300, unaligned = true, disableOperatorChaining = false, timeout = 180)
object KafkaSourceTest extends FlinkStreaming {

  override def process(): Unit = {
    this.fire.createDirectStreamWithZtoDeserialize[OrderInfo]().map(order => {
      println("开始执行")
      println(order.toString)
      println("结束执行")
    }).print()
  }
}


