/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.flink.ext.stream

import com.zto.fire._
import com.zto.fire.common.bean.Generator
import com.zto.fire.common.conf.{FireKafkaConf, FireRocketMQConf, KeyNum}
import com.zto.fire.common.enu.Datasource._
import com.zto.fire.common.enu.{Operation => FOperation}
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.common.lineage.parser.connector.{CustomizeConnectorParser, KafkaConnectorParser, RocketmqConnectorParser}
import com.zto.fire.common.util.MQType.MQType
import com.zto.fire.common.util.{KafkaUtils, MQType, OSUtils, RegularUtils, SQLUtils}
import com.zto.fire.core.Api
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.connector.FlinkConnectors._
import com.zto.fire.flink.ext.provider.{HBaseConnectorProvider, JdbcFlinkProvider}
import com.zto.fire.flink.sql.FlinkSqlExtensionsParser
import com.zto.fire.flink.util.{FlinkRocketMQUtils, FlinkSingletonFactory, FlinkUtils, TableUtils}
import com.zto.fire.jdbc.JdbcConnectorBridge
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema, KeyedDeserializationSchema}
import org.apache.flink.table.api.{StatementSet, Table, TableResult}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.flink.common.serialization.SimpleTagKeyValueDeserializationSchema
import org.apache.rocketmq.flink.{RocketMQConfig, RocketMQSourceWithTag}

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConversions
import scala.reflect.ClassTag

/**
 * 用于对Flink StreamExecutionEnvironment的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class StreamExecutionEnvExt(env: StreamExecutionEnvironment) extends Api with TableApi with JdbcConnectorBridge
  with HBaseConnectorProvider with JdbcFlinkProvider {
  private[fire] lazy val tableEnv = FlinkSingletonFactory.getTableEnv

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
  def createKafkaConsumer[T](kafkaParams: Map[String, Object] = null,
                             topics: Set[String] = null,
                             deserializer: Any = new SimpleStringSchema,
                             keyNum: Int = KeyNum._1): FlinkKafkaConsumer[T] = {
    val confTopics = FireKafkaConf.kafkaTopics(keyNum)
    val topicList = if (StringUtils.isNotBlank(confTopics)) confTopics.split(",") else if (topics != null) topics.toArray else null
    require(topicList != null && topicList.nonEmpty, s"kafka topic不能为空，请在配置文件中指定：kafka.topics$keyNum")

    val confKafkaParams = KafkaUtils.kafkaParams(kafkaParams, FlinkSingletonFactory.getAppName, keyNum = keyNum)
    // 配置文件中相同的key优先级高于代码中的
    require(confKafkaParams.nonEmpty, "kafka相关配置不能为空！")
    require(confKafkaParams.contains("bootstrap.servers"), s"kafka bootstrap.servers不能为空，请在配置文件中指定：kafka.brokers.name$keyNum")
    require(confKafkaParams.contains("group.id"), s"kafka group.id不能为空，请在配置文件中指定：kafka.group.id$keyNum")
    require(deserializer != null, "deserializer不能为空，默认SimpleStringSchema")

    val properties = new Properties()
    confKafkaParams.foreach(t => properties.setProperty(t._1, t._2.toString))

    // 添加topic列表信息
    val topicsStr = topicList.mkString("", ", ", "")
    properties.setProperty("kafka.topics", topicsStr)
    // 添加二次开发相关配置信息
    properties.setProperty(FireKafkaConf.KAFKA_OVERWRITE_STATE_OFFSET, FireKafkaConf.kafkaForceOverwriteStateOffset.toString)
    properties.setProperty(FireKafkaConf.KAFKA_FORCE_AUTO_COMMIT, FireKafkaConf.kafkaForceCommit.toString)
    properties.setProperty(FireKafkaConf.KAFKA_FORCE_AUTO_COMMIT_INTERVAL, FireKafkaConf.kafkaForceCommitInterval.toString)

    // 消费kafka埋点信息
    KafkaConnectorParser.addDatasource(KAFKA, confKafkaParams("bootstrap.servers").toString, topicsStr, confKafkaParams("group.id").toString, FOperation.SOURCE)

    deserializer match {
      case schema: JSONKeyValueDeserializationSchema =>
        new FlinkKafkaConsumer[ObjectNode](JavaConversions.seqAsJavaList(topicList.map(topic => StringUtils.trim(topic))),
          schema, properties).asInstanceOf[FlinkKafkaConsumer[T]]
      case _ =>
        new FlinkKafkaConsumer[String](JavaConversions.seqAsJavaList(topicList.map(topic => StringUtils.trim(topic))),
          new SimpleStringSchema, properties).asInstanceOf[FlinkKafkaConsumer[T]]
    }
  }

  /**
   * 用于反序列化kafka消息中的key与value的包装
   */
  private case class KafkaMessage(key: String, value: String)

  /**
   * 自定义反序列化器，支持反序列化kafka消息中的key与value
   */
  private class KafkaMessageDeserializationSchema extends KafkaDeserializationSchema[KafkaMessage] {
    override def isEndOfStream(nextElement: KafkaMessage): Boolean = false

    override def getProducedType: TypeInformation[KafkaMessage] = {
      TypeInformation.of(classOf[KafkaMessage])
    }

    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): KafkaMessage = {
      val key = new String(record.key(), "UTF-8")
      val value = new String(record.value(), "UTF-8")
      KafkaMessage(key, value)
    }
  }

  /**
   * 可指定支持的deserializer创建DStream流
   *
   * @param kafkaParams
   * kafka相关的配置参数
   * @return
   * DStream
   */
  def createDirectStreamBySchema[T: TypeInformation : ClassTag](kafkaParams: Map[String, Object] = null,
                                                                topics: Set[String] = null,
                                                                specificStartupOffsets: Map[KafkaTopicPartition, java.lang.Long] = null,
                                                                runtimeContext: RuntimeContext = null,
                                                                deserializer: Any = new SimpleStringSchema,
                                                                keyNum: Int = KeyNum._1): DataStream[T] = {

    val kafkaConsumer = this.createKafkaConsumer[T](kafkaParams, topics, deserializer, keyNum)

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
  def createDirectStream(kafkaParams: Map[String, Object] = null,
                         topics: Set[String] = null,
                         specificStartupOffsets: Map[KafkaTopicPartition, java.lang.Long] = null,
                         runtimeContext: RuntimeContext = null,
                         keyNum: Int = KeyNum._1): DataStream[String] = {

    this.createDirectStreamBySchema[String](kafkaParams, topics, specificStartupOffsets, runtimeContext, keyNum = keyNum)
  }

  /**
   * 基于指定的schema创建DStream流
   *
   * @param kafkaParams
   * kafka相关的配置参数
   * @return
   * DStream
   */
  def createDirectStreamByJsonKeyValue(kafkaParams: Map[String, Object] = null,
                                       topics: Set[String] = null,
                                       specificStartupOffsets: Map[KafkaTopicPartition, java.lang.Long] = null,
                                       runtimeContext: RuntimeContext = null,
                                       keyNum: Int = KeyNum._1): DataStream[ObjectNode] = {

    this.createDirectStreamBySchema[ObjectNode](kafkaParams, topics, specificStartupOffsets, runtimeContext, new JSONKeyValueDeserializationSchema(true), keyNum)
  }

  /**
   * 创建DStream流，以SimpleStringSchema进行反序列化
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
                              keyNum: Int = KeyNum._1): DataStream[String] = {
    this.createDirectStream(kafkaParams, topics, specificStartupOffsets, runtimeContext, keyNum)
  }

  /**
   * 创建DStream流，以JSONKeyValueDeserializationSchema进行反序列化
   *
   * @param kafkaParams
   * kafka相关的配置参数
   * @return
   * DStream
   */
  def createKafkaDirectStreamByJsonKeyValue(kafkaParams: Map[String, Object] = null,
                                            topics: Set[String] = null,
                                            specificStartupOffsets: Map[KafkaTopicPartition, java.lang.Long] = null,
                                            runtimeContext: RuntimeContext = null,
                                            keyNum: Int = KeyNum._1): DataStream[ObjectNode] = {
    this.createDirectStreamByJsonKeyValue(kafkaParams, topics, specificStartupOffsets, runtimeContext, keyNum)
  }

  /**
   * 构建RocketMQ拉取消息的DStream流，获取消息中的tag、key以及value
   *
   * @param rocketParam
   * rocketMQ相关消费参数
   * @param groupId
   * groupId
   * @param topics
   * topic列表
   * @return
   * rocketMQ DStream
   */
  def createRocketMqPullStreamWithTag(rocketParam: Map[String, String] = null,
                                      groupId: String = null,
                                      topics: String = null,
                                      tag: String = null,
                                      keyNum: Int = KeyNum._1): DataStream[(String, String, String)] = {
    // 获取topic信息，配置文件优先级高于代码中指定的
    val confTopics = FireRocketMQConf.rocketTopics(keyNum)
    val finalTopics = if (StringUtils.isNotBlank(confTopics)) confTopics else topics
    require(StringUtils.isNotBlank(finalTopics), s"RocketMQ的Topics不能为空，请在配置文件中指定：rocket.topics$keyNum")

    // groupId信息
    val confGroupId = FireRocketMQConf.rocketGroupId(keyNum)
    val finalGroupId = if (StringUtils.isNotBlank(confGroupId)) confGroupId else groupId
    require(StringUtils.isNotBlank(finalGroupId), s"RocketMQ的groupId不能为空，请在配置文件中指定：rocket.group.id$keyNum")

    // 详细的RocketMQ配置信息
    val finalRocketParam = FlinkRocketMQUtils.rocketParams(rocketParam, finalTopics, finalGroupId, rocketNameServer = null, tag = tag, keyNum)
    require(!finalRocketParam.isEmpty, "RocketMQ相关配置不能为空！")
    require(finalRocketParam.containsKey(RocketMQConfig.NAME_SERVER_ADDR), s"RocketMQ nameserver.address不能为空，请在配置文件中指定：rocket.brokers.name$keyNum")

    // 消费rocketmq埋点信息
    RocketmqConnectorParser.addDatasource(ROCKETMQ, finalRocketParam(RocketMQConfig.NAME_SERVER_ADDR), finalTopics, finalGroupId, FOperation.SOURCE)

    val props = new Properties()
    props.putAll(finalRocketParam)

    this.env.addSource(new RocketMQSourceWithTag[(String, String, String)](new SimpleTagKeyValueDeserializationSchema, props)).name("RocketMQ Source")
  }

  /**
   * 构建RocketMQ拉取消息的DStream流，仅获取消息体中的key和value
   *
   * @param rocketParam
   * rocketMQ相关消费参数
   * @param groupId
   * groupId
   * @param topics
   * topic列表
   * @return
   * rocketMQ DStream
   */
  def createRocketMqPullStreamWithKey(rocketParam: Map[String, String] = null,
                                      groupId: String = null,
                                      topics: String = null,
                                      tag: String = null,
                                      keyNum: Int = KeyNum._1): DataStream[(String, String)] = {
    this.createRocketMqPullStreamWithTag(rocketParam, groupId, topics, tag, keyNum).map(t => (t._2, t._3))
  }

  /**
   * 构建RocketMQ拉取消息的DStream流，仅获取消息体中的value
   *
   * @param rocketParam
   * rocketMQ相关消费参数
   * @param groupId
   * groupId
   * @param topics
   * topic列表
   * @return
   * rocketMQ DStream
   */
  def createRocketMqPullStream(rocketParam: Map[String, String] = null,
                               groupId: String = null,
                               topics: String = null,
                               tag: String = null,
                               keyNum: Int = KeyNum._1): DataStream[String] = {
    this.createRocketMqPullStreamWithTag(rocketParam, groupId, topics, tag, keyNum).map(t => t._3)
  }

  /**
   * 精简版的消费KAFKA或ROCKETMQ的api，可根据mqType参数进行主动设置
   *
   * @param mqType
   * auto：表示自动根据配置消费kafka或rocketmq，比如使用@Kafka注解，则消费kafka
   * 注：如果同时指定@Kafka和@Rocketmq且keyNum相同，则会报错
   * kafka：强制设置mq类型为kafka，则只会去消费kafka
   * rocketmq：强制设置rocketmq，则只会消费rocketmq
   * @param keyNum
   * 用于区分不同的mq源
   * @return
   * key & body
   */
  def createMQStreamWithKey(mqType: MQType = MQType.auto, keyNum: Int = KeyNum._1): DataStream[(String, String)] = {
    def kafkaStream: DataStream[(String, String)] = this.createKafkaDirectStream(keyNum = keyNum).asInstanceOf[DataStream[KafkaMessage]].map(t => (t.key, t.value))

    def rocketStream: DataStream[(String, String)] = this.createRocketMqPullStreamWithKey(keyNum = keyNum)

    mqType match {
      case MQType.kafka => kafkaStream
      case MQType.rocketmq => rocketStream

      case _ => {
        val kafkaTopicValue = FireKafkaConf.kafkaBrokers(keyNum)
        val rocketTopicValue = FireRocketMQConf.rocketNameServer(keyNum)

        // 根据配置文件区分不同的消费场景（消费kafka还是rocketmq）
        if (noEmpty(kafkaTopicValue) && noEmpty(rocketTopicValue)) {
          throw new IllegalArgumentException(s"kafka和rocketmq对应的连接参数均未指定，自动推断失败！keyNum=${keyNum}")
        }

        if (isEmpty(kafkaTopicValue) && isEmpty(rocketTopicValue)) {
          throw new IllegalArgumentException(s"kafka和rocketmq对应的连接参数同时被指定，自动推断失败！keyNum=${keyNum}")
        }

        if (noEmpty(kafkaTopicValue)) kafkaStream else rocketStream
      }
    }
  }

  /**
   * 精简版的消费KAFKA或ROCKETMQ的api，可根据mqType参数进行主动设置
   *
   * @param mqType
   * auto：表示自动根据配置消费kafka或rocketmq，比如使用@Kafka注解，则消费kafka
   * 注：如果同时指定@Kafka和@Rocketmq且keyNum相同，则会报错
   * kafka：强制设置mq类型为kafka，则只会去消费kafka
   * rocketmq：强制设置rocketmq，则只会消费rocketmq
   * @param keyNum
   * 用于区分不同的mq源
   * @return
   * key & body
   */
  def createMQStream(mqType: MQType = MQType.auto, keyNum: Int = KeyNum._1): DataStream[String] = {
    this.createMQStreamWithKey(mqType, keyNum).map(_._2)
  }

/*

  /**
   * 创建自定义数据生成规则的DStream流
   * 调用者需通过定义gen函数确定具体数据的生成逻辑
   *
   * @param gen
   * 数据生成策略的函数
   * @param qps
   * 数据生成的qps
   * @return
   * ReceiverInputDStream[T]
   */
  def createGenStream[T](gen: => T, qps: Long = 1000): DataStream[T] = {
    this.addSource(new GenConnector(gen, qps))
  }

  /**
   * 创建JavaBean DStream流，JavaBean必须实现Generator接口
   *
   * @param qps
   * 数据生成的qps
   * @tparam T
   * 生成数据的类型
   * @return
   * DataStream[T]
   */
  def createBeanStream[T <: Generator[T] : ClassTag](qps: Long = 1000): DataStream[Generator[T]] = {
    this.addSource(new BeanConnector[T](100))
  }
*/


  /**
   * 创建Int型数据随机数DStream
   *
   * @param qps
   * 数据生成的qps
   * @return
   * ReceiverInputDStream[T]
   */
  def createRandomIntStream(qps: Long = 1000): DataStream[Int] = {
    this.addSource(new RandomIntConnector(qps))
  }

  /**
   * 创建Long型数据随机数DStream
   *
   * @param qps
   * 数据生成的qps
   * @return
   * DataStream[T]
   */
  def createRandomLongStream(qps: Long = 1000): DataStream[Long] = {
    this.addSource(new RandomLongConnector(qps))
  }

  /**
   * 创建Double型数据随机数DStream
   *
   * @param qps
   * 数据生成的qps
   * @return
   * DataStream[T]
   */
  def createRandomDoubleStream(qps: Long = 1000): DataStream[Double] = {
    this.addSource(new RandomDoubleConnector(qps))
  }

  /**
   * 创建Float型数据随机数DStream
   *
   * @param qps
   * 数据生成的qps
   * @return
   * DataStream[T]
   */
  def createRandomFloatStream(qps: Long = 1000): DataStream[Float] = {
    this.addSource(new RandomFloatConnector(qps))
  }

  /**
   * 创建根据指定规则生成对象实例的DataGenReceiver
   *
   * @param qps
   * 数据生成的qps
   * @return
   * DataStream[T]
   */
  def createUUIDStream(qps: Long = 1000): DataStream[String] = {
    this.addSource(new UUIDConnector(qps))
  }

  /**
   * 创建基于JavaBean序列化JSON DStream流，JavaBean必须实现Generator接口
   *
   * @param qps
   * 数据生成的qps
   * @tparam T
   * 生成数据的类型
   * @return
   * DataStream[T]
   */
  def createJSONStream[T <: Generator[T] : ClassTag](qps: Long = 1000): DataStream[String] = {
    this.addSource(new JSONConnector[T](100))
  }

  /**
   * 自定义Source
   */
  def addSource[T: TypeInformation](function: SourceFunction[T]): DataStream[T] = {
    CustomizeConnectorParser.addDatasource(CUSTOMIZE_SOURCE, OSUtils.getIp, function.getClass.getSimpleName, FOperation.SOURCE)
    this.env.addSource[T](function)
  }

  /**
   * 执行sql query操作
   *
   * @param sql
   * sql语句
   * @return
   * table对象
   */
  def sqlQuery(sql: String): Table = {
    SQLUtils.executeSql(sql) ( statement => this.tableEnv.sqlQuery(FlinkUtils.sqlWithReplace(statement))).get
  }

  /**
   * 执行sql语句
   * 支持DDL、DML
   */
  def sql(sql: String): TableResult = {
    SQLUtils.executeSql(sql) { statement =>
      if (FireFlinkConf.autoAddStatementSet && this.isInsertStatement(statement)) {
        FlinkSqlExtensionsParser.sqlParse(statement)
        this.addInsertSql(statement)
        TableUtils.TABLE_RESULT_OK
      } else {
        val finalSql = FlinkUtils.sqlWithReplace(statement)
        FlinkSqlExtensionsParser.sqlParse(finalSql)
        this.tableEnv.executeSql(finalSql)
      }
    }.get
  }

  /**
   * 创建并返回StatementSet对象实例
   */
  def createStatementSet: StatementSet = StreamExecutionEnvExt.createStatementSet

  /**
   * 使用正则匹配执行的sql语句是否为insert语句
   */
  private[this] def isInsertStatement(sql: String): Boolean = {
    RegularUtils.insertReg.findFirstIn(sql.toUpperCase).isDefined
  }

  /**
   * 将待执行的sql sink语句加入到StatementSet中
   *
   * @param sql
   * insert xxx语句
   * @return
   * StatementSet
   */
  def addInsertSql(sql: String): StatementSet = {
    StreamExecutionEnvExt.useStatementSet.compareAndSet(false, true)
    SQLUtils.executeSql(sql) (sql => StreamExecutionEnvExt.statementSet.addInsertSql(sql)).get
  }


  /**
   * addInsertSql方法的别名，将待执行的sql sink语句加入到StatementSet中
   *
   * @param sql
   * insert xxx语句
   * @return
   * StatementSet
   */
  def sqlSink(sql: String): StatementSet = {
    this.addInsertSql(sql)
  }

  /**
   * addInsertSql方法的别名，将待执行的sql sink语句加入到StatementSet中
   *
   * @param sql
   * insert xxx语句
   * @return
   * StatementSet
   */
  def sqlInsert(sql: String): StatementSet = this.addInsertSql(sql)

  /**
   * 将table sink加入到StatementSet中
   */
  def addInsert(targetPath: String, table: Table, overwrite: Boolean = false): StatementSet = {
    StreamExecutionEnvExt.useStatementSet.compareAndSet(false, true)
    StreamExecutionEnvExt.statementSet.addInsert(targetPath, table, overwrite)
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
  def startAwaitTermination(jobName: String = FlinkSingletonFactory.getAppName): Any = {
    if (StreamExecutionEnvExt.useStatementSet.get()) StreamExecutionEnvExt.statementSet.execute() else this.env.execute(jobName)
  }

  /**
   * 提交Flink Streaming Graph并执行
   */
  def start(jobName: String): Any = this.startAwaitTermination(jobName)

  /**
   * 流的启动
   */
  override def start: Any = this.startAwaitTermination()
}

private[fire] object StreamExecutionEnvExt {
  private[fire] lazy val statementSet = this.createStatementSet
  private[fire] lazy val useStatementSet = new AtomicBoolean(false)

  /**
   * 创建并返回StatementSet对象实例
   */
  def createStatementSet: StatementSet = FlinkSingletonFactory.getTableEnv.createStatementSet()
}