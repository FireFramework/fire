package com.zto.fire.flink.ext.stream

import java.util.Properties

import com.zto.fire._
import com.zto.fire.common.conf.FireKafkaConf
import com.zto.fire.common.util.{KafkaUtils, ValueUtils}
import com.zto.fire.core.Api
import com.zto.fire.flink.ext.provider.{HBaseConnectorProvider, JdbcFlinkProvider}
import com.zto.fire.flink.util.FlinkSingletonFactory
import com.zto.fire.jdbc.JdbcConnectorBridge
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.table.api.{Table, TableResult}

import scala.collection.JavaConversions

/**
 * 用于对Flink StreamExecutionEnvironment的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class StreamExecutionEnvExt(env: StreamExecutionEnvironment) extends Api with JdbcConnectorBridge
  with HBaseConnectorProvider with JdbcFlinkProvider {
  private[fire] lazy val tableEnv = FlinkSingletonFactory.getStreamTableEnv

  /**
   * 创建Socket流
   */
  def createSocketTextStream(hostname: String, port: Int, delimiter: Char = '\n', maxRetry: Long = 0): DataStream[String] = {
    this.env.socketTextStream(hostname, port, delimiter, maxRetry)
  }

  /**
   * 根据配置信息创建Kafka Consumer
   *
   * @param kafkaParams
   * kafka相关的配置参数
   * @return
   * FlinkKafkaConsumer011
   */
  def createKafkaConsumer(kafkaParams: Map[String, Object] = null,
                          topics: Set[String] = null,
                          keyNum: Int = 1): FlinkKafkaConsumer[String] = {
    val confTopics = FireKafkaConf.kafkaTopics(keyNum)
    val topicList = if (StringUtils.isNotBlank(confTopics)) confTopics.split(",") else topics.toArray
    require(topicList != null && topicList.nonEmpty, s"kafka topic不能为空，请在配置文件中指定：flink.kafka.topics$keyNum")

    val confKafkaParams = KafkaUtils.kafkaParams(kafkaParams, FlinkSingletonFactory.getAppName, keyNum = keyNum)
    // 配置文件中相同的key优先级高于代码中的
    require(confKafkaParams.nonEmpty, "kafka相关配置不能为空！")
    val properties = new Properties()
    confKafkaParams.foreach(t => properties.setProperty(t._1, t._2.toString))

    val kafkaConsumer = new FlinkKafkaConsumer[String](JavaConversions.seqAsJavaList(topicList.map(topic => StringUtils.trim(topic))),
      new SimpleStringSchema(), properties)
    kafkaConsumer
  }

  /**
   * 创建DStream流
   *
   * @param kafkaParams
   * kafka相关的配置参数
   * @return
   * DStream
   */
  def createDirectStream(kafkaParams: Map[String, Object] = null,
                         topics: Set[String] = null,
                         specificStartupOffsets: Map[KafkaTopicPartition, java.lang.Long] = null,
                         runtimeContext: RuntimeContext = null,
                         keyNum: Int = 1): DataStream[String] = {

    val kafkaConsumer = this.createKafkaConsumer(kafkaParams, topics, keyNum)

    if (runtimeContext != null) kafkaConsumer.setRuntimeContext(runtimeContext)
    if (specificStartupOffsets != null) kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets)
    // 设置从指定时间戳位置开始消费kafka
    val startFromTimeStamp = FireKafkaConf.kafkaStartFromTimeStamp(keyNum)
    if (startFromTimeStamp > 0) kafkaConsumer.setStartFromTimestamp(FireKafkaConf.kafkaStartFromTimeStamp(keyNum))
    // 是否在checkpoint时记录offset值
    kafkaConsumer.setCommitOffsetsOnCheckpoints(FireKafkaConf.kafkaCommitOnCheckpoint(keyNum))
    // 设置从最早的位置开始消费
    if (FireKafkaConf.offsetSmallest.equalsIgnoreCase(FireKafkaConf.kafkaStartingOffset(keyNum))) kafkaConsumer.setStartFromEarliest()
    // 设置从最新位置开始消费
    if (FireKafkaConf.offsetLargest.equalsIgnoreCase(FireKafkaConf.kafkaStartingOffset(keyNum))) kafkaConsumer.setStartFromLatest()
    // 从topic中指定的group上次消费的位置开始消费，必须配置group.id参数
    if (FireKafkaConf.kafkaStartFromGroupOffsets(keyNum)) kafkaConsumer.setStartFromGroupOffsets()

    this.env.addSource(kafkaConsumer)
  }

  /**
   * 创建DStream流
   *
   * @param kafkaParams
   * kafka相关的配置参数
   * @return
   * DStream
   */
  def createKafkaDirectStream(kafkaParams: Map[String, Object] = null,
                              topics: Set[String] = null,
                              specificStartupOffsets: Map[KafkaTopicPartition, java.lang.Long] = null,
                              runtimeContext: RuntimeContext = null,
                              keyNum: Int = 1): DataStream[String] = {
    this.createDirectStream(kafkaParams, topics, specificStartupOffsets, runtimeContext, keyNum)
  }

  /**
   * 执行sql query操作
   *
   * @param sql
   * sql语句
   * @param keyNum
   * 指定sql的with列表对应的配置文件中key的值，如果为<0则表示不从配置文件中读取with表达式
   * @return
   * table对象
   */
  def sqlQuery(sql: String, keyNum: Int = 1): Table = {
    require(StringUtils.isNotBlank(sql), "待执行的sql语句不能为空")
    this.tableEnv.sqlQuery(sql.with$(keyNum))
  }

  /**
   * 执行sql语句
   * 支持DDL、DML
   * @param keyNum
   * 指定sql的with列表对应的配置文件中key的值，如果为<0则表示不从配置文件中读取with表达式
   */
  def sql(sql: String, keyNum: Int = 1): TableResult = {
    require(StringUtils.isNotBlank(sql), "待执行的sql语句不能为空")
    this.tableEnv.executeSql(sql.with$(keyNum))
  }

  /**
   * 使用集合元素创建DataStream
   *
   * @param seq
   * 元素集合
   * @tparam T
   * 元素的类型
   */
  def parallelize[T: TypeInformation](seq: Seq[T]): DataStream[T] = {
    this.env.fromCollection[T](seq)
  }

  /**
   * 使用集合元素创建DataStream
   *
   * @param seq
   * 元素集合
   * @tparam T
   * 元素的类型
   */
  def createCollectionStream[T: TypeInformation](seq: Seq[T]): DataStream[T] = this.env.fromCollection[T](seq)

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
   * 提交Flink Streaming Graph并执行
   */
  def start(jobName: String): JobExecutionResult = this.startAwaitTermination(jobName)

  /**
   * 流的启动
   */
  override def start: JobExecutionResult = this.env.execute(FlinkSingletonFactory.getAppName)
}
