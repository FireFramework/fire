package com.zto.fire.demo;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 用于测试Java代码
 *
 * @author ChengLong 2019-9-4 13:39:36
 */
public class SendMsgToKafka {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.9.45.97:9092,10.9.15.38:9092,10.9.36.49:9092,10.9.36.50:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        while (true) {
            producer.send(new ProducerRecord<>("fire3", "{\"age\":1,\"className\":\"Student\",\"id\":1,\"length\":33.16,\"name\":\"admin\", \"createTime\": \"2020-04-17 17:52:00\"}"));
            producer.send(new ProducerRecord<>("fire", "{\"age\":1,\"className\":\"Student\",\"id\":1,\"length\":33.16,\"name\":\"admin\", \"createTime\": \"2020-04-17 17:52:00\"}"));
            producer.send(new ProducerRecord<>("flink", "{\"age\":1,\"className\":\"Student\",\"id\":1,\"length\":33.16,\"name\":\"admin\", \"createTime\": \"2020-04-17 17:52:00\"}"));
            Thread.sleep(10);
        }
    }

}