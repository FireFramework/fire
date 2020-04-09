package com.zto.fire.flink.core.ext.stream

import java.util.Properties

import com.zto.fire.common.util.{GlobalConstants, ValueUtils}
import com.zto.fire.core.util.SparkUtils
import com.zto.fire.flink.core.util.FlinkSingletonFactory
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.JavaConversions

/**
 * 用于对Flink StreamExecutionEnvironment的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class StreamExecutionEnvExt(env: StreamExecutionEnvironment) {

  /**
   * 创建DStream流
   *
   * @param kafkaParams
   * kafka相关的配置参数
   * @return
   * DStream
   */
  def createDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, keyNum: Int = 1): DataStream[String] = {
    val groupId = GlobalConstants.KafkaConf.kafkaGroupId(keyNum)

    // 配置文件中的group.id优先级更高，若位置的，则取当前appName
    val finalGroupId = if (ValueUtils.isNotEmpty(groupId)) groupId else FlinkSingletonFactory.getAppName
    val kafkaProps = if (ValueUtils.isNotEmpty(kafkaParams)) kafkaParams else SparkUtils.kafkaParams(finalGroupId, keyNum = keyNum)
    ValueUtils.requireNonNullForce(kafkaProps, "kafka相关配置不能为空！")
    val topicList = if (ValueUtils.isNotEmpty(topics)) topics.toArray else GlobalConstants.KafkaConf.kafkaTopics(keyNum).split(",")

    val properties = new Properties();
    kafkaProps.foreach(t => properties.setProperty(t._1, t._2.toString))

    val kafkaConsumer = new FlinkKafkaConsumer010[String](JavaConversions.seqAsJavaList(topicList.map(topic => StringUtils.trim(topic))),
      new SimpleStringSchema(), properties)
    env.addSource(kafkaConsumer)
  }

  /**
   * 提交job执行
   *
   * @param jobName
   * job名称
   */
  def startAwaitTermination(jobName: String = ""): JobExecutionResult = {
    if (ValueUtils.isEmpty(jobName)) this.env.execute() else this.env.execute(jobName)
  }

  /**
   * 使用集合元素创建DataStream
   * @param seq
   *            元素集合
   * @tparam T
   *           元素的类型
   */
  def parallelize[T: TypeInformation](seq: Seq[T]): DataStream[T] = {
    this.env.fromCollection[T](seq)
  }
}
