
package com.zto.fire.examples.flink.serialize

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala.createTypeInformation


/**
 * @author zhanggougou 2025-12-08 10:09:19
 */
@Config(
  """
    |flink.rocket.brokers.name=testmq02ns1.test.ztosys.com:9876; testmq02ns2.test.ztosys.com:9876;testmq02ns3.test.ztosys.com:9876
    |flink.rocket.topics=fire_test_rocket_serialize
    |flink.rocket.group.id=fire_test_rocket_serialize_consumer
    |flink.rocket.starting.offsets=earliest
    |flink.rocket.consumer.tag=*
    |""")
@Streaming(interval = 300, unaligned = true, disableOperatorChaining = false, timeout = 180)
object RocketSourceTest extends FlinkStreaming {

  override def process(): Unit = {
    this.fire.createRocketMqPullStreamWithEntity[OrderInfo]().map(order => {
      println("开始执行")
      println(order.toString)
      println("结束执行")
    }).print()
  }
}


