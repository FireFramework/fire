package com.zto.bigdata.spark.common.mq

import java.util

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer
import com.alibaba.rocketmq.client.consumer.listener._
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere
import com.alibaba.rocketmq.common.message.MessageExt
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * 用于接收RocketMQ发送过来的数据
  * Created by ChengLong on 2017-02-13.
  */
class RocketMQReceiver(val url: String, val consumerName: String, val topic: String) extends Receiver[Array[Byte]](StorageLevel.MEMORY_AND_DISK_SER) {
  @transient private var consumer: DefaultMQPushConsumer = null

  override def onStart(): Unit = {
    consumer = new DefaultMQPushConsumer(consumerName)
    consumer.setNamesrvAddr(url)
    consumer.subscribe(topic, null)
    //程序第一次启动从消息队列头取数据
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
    println("=================== start receive message ===================")
    new Thread(this.consumerName) {
      override def run() {
        receive()
      }
    }.start()
  }

  /**
    * 接收数据逻辑
    */
  private def receive(): Unit = {
    consumer.registerMessageListener(new MessageListenerOrderly() {
      override def consumeMessage(msgs: util.List[MessageExt], context: ConsumeOrderlyContext): ConsumeOrderlyStatus = {
        import scala.collection.JavaConversions._
        try {
          for (msg <- msgs) {
            store(msg.getBody)
          }
          if(isStopped()) {
            restart(s"restart $consumerName")
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
          } else {
            return ConsumeOrderlyStatus.SUCCESS
          }
        } catch {
          case e: Exception => {
            e.printStackTrace()
            if(isStopped()) {
              restart(s"restart $consumerName")
            }
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
          }
        }
      }
    })
    consumer.start()
  }

  override def onStop(): Unit = {
    println("=================== stop receive message ===================")
  }
}