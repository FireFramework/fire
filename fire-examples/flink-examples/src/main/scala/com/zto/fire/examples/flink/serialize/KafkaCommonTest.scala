package com.zto.fire.examples.flink.serialize

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala._

import java.util.Date

/**
 * 测试kafka通用api
 * @author zhanggougou 2025-12-08 10:09:19
 */
@Config(
  """
    |kafka.brokers.name=10.7.114.156:9092,10.7.114.157:9092,10.7.114.158:9092
    |kafka.topics=fire-test-kafka
    |kafka.group.id=fire-test-kafka-consumer
    |kafka.starting.offsets=earliest
    |""")
@Streaming(interval = 300, unaligned = true, disableOperatorChaining = false, timeout = 180)
object KafkaCommonTest extends FlinkStreaming {

  override def process(): Unit = {
    this.fire.createRandomIntStream(1).map(x => {
        val orderInfo = new OrderInfo(x, x + "我是kafka common test", new Date(), 1.6 + x)
        MQRecord(JSONUtils.toJSONString(orderInfo))
      })
      .sinkKafka()
      .uname("sink-kafka-common")

     this.fire.createKafkaDirectStream()
      .uname("source-kafka-common").map(x => {
         JSONUtils.parseObject(x, classOf[OrderInfo])
       }).print().name("print-kafka-common")

  }
}


