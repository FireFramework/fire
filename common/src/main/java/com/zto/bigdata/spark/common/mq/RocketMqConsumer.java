package com.zto.bigdata.spark.common.mq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.Serializable;
import java.util.List;

public class RocketMqConsumer implements Serializable {
    private static final long serialVersionUID = 1L;
    public static void main(String args[]) {

        DefaultMQPushConsumer consumer =
                new DefaultMQPushConsumer("ConsumerGroupName");
        // consumer.setNamesrvAddr("192.168.126.100:9876");
        consumer.setNamesrvAddr("10.10.4.111:9876");
        try {
            //订阅PushTopic下Tag为push的消息
            consumer.subscribe("spark", "*");
            //程序第一次启动从消息队列头取数据
            consumer.setConsumeFromWhere(
                    ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        System.out.println(new String(msg.getBody()));
                        // receiver.store(new String(msg.getBody()));
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}