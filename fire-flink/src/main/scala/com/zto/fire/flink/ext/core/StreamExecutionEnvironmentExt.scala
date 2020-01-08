package com.zto.fire.flink.ext.core

import java.util.Properties

import com.zto.fire.common.util.ValueUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
 * 用于对Flink StreamExecutionEnvironment的API库扩展
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class StreamExecutionEnvironmentExt(env: StreamExecutionEnvironment) {

  def createDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, keyNum: Int = 1): DataStream[String] = {
    val properties = new Properties();
    //kafka的节点的IP或者hostName，多个使用逗号分隔
    properties.setProperty("bootstrap.servers", "10.9.45.97:9092,10.9.15.38:9092,10.9.36.49:9092,10.9.36.50:9092");
    //zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
    properties.setProperty("group.id", "fire2");
    //此处三个参数上面已经讲过，这里用的是SimpleStringSchema这种方式反序列化，后用fastJson转成json进行处理
    val myConsumer = new FlinkKafkaConsumer010[String]("fire",
      new SimpleStringSchema(), properties);
    env.addSource(myConsumer)
  }

  def startAwaitTermination(jobName: String = ""): Unit = {
    if (ValueUtils.isEmpty(jobName)) this.env.execute() else this.env.execute(jobName)
  }
}
