package com.zto.bigdata.spark.common.mq;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.zto.bigdata.spark.common.util.DateFormatUtils;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Created by ChengLong on 2017-01-13.
 */
public class RocketMqProducer {
    private static DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
    private static int initialState = 0;

    private RocketMqProducer() {
    }

    public static DefaultMQProducer getDefaultMQProducer() {
        if (producer == null) {
            producer = new DefaultMQProducer("ProducerGroupName");
        }

        if (initialState == 0) {
            // producer.setNamesrvAddr("192.168.126.100:9876");
            producer.setNamesrvAddr("10.10.4.111:9876");
            try {
                producer.start();
            } catch (MQClientException e) {
                e.printStackTrace();
                return null;
            }
            initialState = 1;
        }
        return producer;
    }

    public static void sendMsg() {
        // 获取消息生产者
        DefaultMQProducer producer = RocketMqProducer.getDefaultMQProducer();
        try {
            BigDecimal count = new BigDecimal(121.1235);
            count = count.setScale(BigDecimal.ROUND_HALF_UP, 4);
            for (int i = 0; i < 10000; i++) {
                count = count.add(new BigDecimal(i));
                String date = DateFormatUtils.formatDateTime(new Date());
                // System.out.println(json);
                Message msg = new Message(
                        "spark",
                        "spark",
                        "spark",
                        date.getBytes());
                SendResult sendResult = producer.send(msg);
                Thread.sleep(1000);
            }
            /*for (int i = 0; i < Integer.MAX_VALUE; i++) {
                Message msg = new Message(
                        "spark",
                        "TagA",
                        "OrderID00" + i,
                        ("Hello MetaQ" + i).getBytes());
                SendResult sendResult = producer.send(msg);
                System.out.println("send: " + msg);
                Thread.sleep(3000);
                //logger.info("sendResult:{}", sendResult);
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.shutdown();
    }

    public static void main(String[] args) throws Exception {
        sendMsg();
    }
}
