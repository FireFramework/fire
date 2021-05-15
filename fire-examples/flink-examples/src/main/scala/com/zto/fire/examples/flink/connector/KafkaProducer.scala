package com.zto.fire.examples.flink.connector

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer {

  def main(args: Array[String]): Unit = {

    val kafkaProp = new Properties()
    val servers = "10.9.46.111:9092";
    kafkaProp.put("bootstrap.servers", servers)
    kafkaProp.put("acks", "1")
    kafkaProp.put("retries", "3")
    kafkaProp.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProp.put("value.serializer", classOf[StringSerializer].getName)

    val topic = "kafka_hudi_test"

    val producer = new KafkaProducer[String, String](kafkaProp)

    for(id <- 1 to 100){

      Thread.sleep(1000)

      val ts = System.currentTimeMillis()
      val dataStr = new SimpleDateFormat("yyyyMMdd").format(ts)

      val value = "{\"uuid\" : " + id + ", \"action\":\"action" + id + "\", \"age\": 18, \"ts\": " + ts + " ,\"ds\" : " + dataStr + "}"

      val record = new ProducerRecord[String, String](topic, value)
      producer.send(record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (metadata != null) {
            println("发送数据到kafka成功:" + value)
          }
          if (exception != null) {
            println(exception)
            println("消息发送到kafka失败:" + value)
          }
        }
      })
    }

    producer.close()

  }

}
