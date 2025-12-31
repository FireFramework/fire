package com.zto.fire.examples.flink.serialize

import com.zto.bigdata.integeration.util.ZtoSerializeUtil
import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.conf.FireRocketMQConf
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import com.zto.fire.flink.conf.{SerializeType, ZtoSerializeConf}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import java.util.Date

/**
 * @author zhanggougou 2025-12-08 10:09:19
 */
@Config(
  """
    |flink.rocket.brokers.name=testmq02ns1.test.ztosys.com:9876; testmq02ns2.test.ztosys.com:9876;testmq02ns3.test.ztosys.com:9876
    |flink.rocket.topics=fire_test_rocket
    |flink.rocket.group.id=fire_test_rocket_consumer
    |flink.rocket.starting.offsets=earliest
    |flink.rocket.consumer.tag=*
    |""")
@Streaming(interval = 300, unaligned = true, disableOperatorChaining = false, timeout = 180)
object RocketCommonTest extends FlinkStreaming {

  override def process(): Unit = {
    this.fire.createRandomIntStream(1).map(x => {
        val orderInfo = new OrderInfo(x, x + "我是rocket common test", new Date(), 1.7 + x)
        MQRecord(JSONUtils.toJSONString(orderInfo))
      })
      .sinkRocketMQ()
      .uname("sink-rocket-common")

    this.fire.createRocketMqPullStream()
      .uname("source-rocket-common").map(x => {
        JSONUtils.parseObject(x, classOf[OrderInfo])
      }).print().name("print-rocket-common")

  }
}


