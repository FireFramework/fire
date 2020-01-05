package com.zto.fire.flink

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

object FlinkTest {

  def main(args: Array[String]): Unit = {
    val conf: Configuration = new Configuration()
    import org.apache.flink.configuration.ConfigConstants
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.enableCheckpointing(500)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    val properties = new Properties();
    //kafka的节点的IP或者hostName，多个使用逗号分隔
    properties.setProperty("bootstrap.servers", "10.9.45.97:9092,10.9.15.38:9092,10.9.36.49:9092,10.9.36.50:9092");
    //zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
    properties.setProperty("group.id", "fire");
    //此处三个参数上面已经讲过，这里用的是SimpleStringSchema这种方式反序列化，后用fastJson转成json进行处理
    val myConsumer = new FlinkKafkaConsumer010[String]("fire",
      new SimpleStringSchema(), properties);

    val keyedStream = env.addSource(myConsumer)
    val studentStream = keyedStream.flatMap(json => {
      val list = ListBuffer[String]()
      list += json
      list += json
      list
    })
    studentStream.print()

    env.execute("kafka test")
  }
}
