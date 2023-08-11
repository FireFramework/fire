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
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.MQRecord
import com.zto.fire.common.conf.{FireKafkaConf, FireRocketMQConf, KeyNum}
import com.zto.fire.common.enu.{Datasource, Operation}
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.common.lineage.parser.connector.KafkaConnector
import com.zto.fire.common.util.MQType.MQType
import com.zto.fire.common.util._
import com.zto.fire.flink.sink.{HBaseSink, JdbcSink, KafkaSink, RocketMQSink}
import com.zto.fire.flink.util.FlinkSingletonFactory
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.bean.HBaseBaseBean
import com.zto.fire.jdbc.JdbcConf
import com.zto.fire.jdbc.conf.FireJdbcConf
import com.zto.fire.jdbc.util.DBUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.accumulators.SimpleAccumulator
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions.JDBCExactlyOnceOptionsBuilder
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExactlyOnceOptions, JdbcExecutionOptions, JdbcStatementBuilder}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.flink.util.function.SerializableSupplier

import java.lang.reflect.Field
import java.sql.PreparedStatement
import javax.sql.XADataSource
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * 用于对Flink DataStream的API库扩展
 *
 * @author ChengLong 2020年1月7日 09:18:21
 * @since 0.4.1
 */
class DataStreamExt[T](stream: DataStream[T]) extends Logging {
  lazy val tableEnv = FlinkSingletonFactory.getTableEnv.asInstanceOf[StreamTableEnvironment]

  /**
   * 将流注册为临时表
   *
   * @param tableName
   * 临时表的表名
   */
  def createOrReplaceTempView(tableName: String): Table = {
    val table = this.stream.toTable(this.tableEnv)
    this.tableEnv.createTemporaryView(tableName, table)
    table
  }

  /**
   * 为当前DataStream设定uid与name
   *
   * @param uid
   * uid
   * @param name
   * name
   * @return
   * 当前实例
   */
  def uname(uid: String, name: String = ""): DataStream[T] = {
    if (noEmpty(uid)) stream.uid(uid)
    if (noEmpty(name)) stream.name(name) else stream.name(uid)
    this.stream
  }

  /**
   * 预先注册flink累加器
   *
   * @param acc
   * 累加器实例
   * @param name
   * 累加器名称
   * @return
   * 注册累加器之后的流
   */
  def registerAcc(acc: SimpleAccumulator[_], name: String): DataStream[String] = {
    this.stream.map(new RichMapFunction[T, String] {
      override def open(parameters: Configuration): Unit = {
        this.getRuntimeContext.addAccumulator(name, acc)
      }

      override def map(value: T): String = value.toString
    })
  }

  /**
   * 将流映射为批流
   *
   * @param count
   * 将指定数量的合并为一个集合
   */
  def countWindowSimple[T: ClassTag](count: Long): DataStream[List[T]] = {
    implicit val typeInfo = TypeInformation.of(classOf[List[T]])
    stream.asInstanceOf[DataStream[T]].countWindowAll(Math.abs(count)).apply(new AllWindowFunction[T, List[T], GlobalWindow]() {
      override def apply(window: GlobalWindow, input: Iterable[T], out: Collector[List[T]]): Unit = {
        out.collect(input.toList)
      }
    })(typeInfo)
  }

  /**
   * 设置并行度
   */
  def repartition(parallelism: Int): DataStream[T] = {
    this.stream.setParallelism(parallelism)
  }

  /**
   * 将DataStream转为Table
   */
  def toTable: Table = {
    this.tableEnv.fromDataStream(this.stream)
  }

  /**
   * jdbc批量sink操作，根据用户指定的DataStream中字段的顺序，依次填充到sql中的占位符所对应的位置
   * 若DataStream为DataStream[Row]类型，则fields可以为空，但此时row中每列的顺序要与sql占位符顺序一致，数量和类型也要一致
   * 注：
   *  1. fieldList指定DataStream中JavaBean的字段名称，非jdbc表中的字段名称
   *     2. fieldList多个字段使用逗号分隔
   *     3. fieldList中的字段顺序要与sql中占位符顺序保持一致，数量一致
   *
   * @param sql
   * 增删改sql
   * @param fields
   * DataStream中数据的每一列的列名（非数据库中的列名，需与sql中占位符的顺序一致）
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  @deprecated("use stream.sinkJdbc", "fire 2.3.3")
  def jdbcBatchUpdate(sql: String,
                      fields: Seq[String],
                      batch: Int = 10,
                      flushInterval: Long = 1000,
                      keyNum: Int = KeyNum._1): DataStreamSink[T] = {
    this.stream.addSink(new JdbcSink[T](sql, batch = batch, flushInterval = flushInterval, keyNum = keyNum) {
      var fieldMap: java.util.Map[String, Field] = _
      var clazz: Class[_] = _

      override def map(value: T): Seq[Any] = {
        requireNonEmpty(sql)("sql语句不能为空")

        val params = ListBuffer[Any]()
        if (value.isInstanceOf[Row] || value.isInstanceOf[Tuple2[Boolean, Row]]) {
          // 如果是Row类型的DataStream[Row]
          val row = if (value.isInstanceOf[Row]) value.asInstanceOf[Row] else value.asInstanceOf[Tuple2[Boolean, Row]]._2
          for (i <- 0 until row.getArity) {
            params += row.getField(i)
          }
        } else {
          requireNonEmpty(fields)("字段列表不能为空！需按照sql中的占位符顺序依次指定当前DataStream中数据字段的名称")

          if (clazz == null && value != null) {
            clazz = value.getClass
            fieldMap = ReflectionUtils.getAllFields(clazz)
          }

          fields.foreach(fieldName => {
            val field = this.fieldMap.get(StringUtils.trim(fieldName))
            requireNonEmpty(field)(s"当前DataStream中不存在该列名$fieldName，请检查！")
            params += field.get(value)
          })
        }
        params
      }
    }).uid("jdbcBatchUpdate")
  }

  /**
   * jdbc批量sink操作
   *
   * @param sql
   * 增删改sql
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  @deprecated("use stream.sinkJdbc", "fire 2.3.3")
  def jdbcBatchUpdate2(sql: String,
                       batch: Int = 10,
                       flushInterval: Long = 1000,
                       keyNum: Int = KeyNum._1)(fun: T => Seq[Any]): DataStreamSink[T] = {
    this.stream.addSink(new JdbcSink[T](sql, batch = batch, flushInterval = flushInterval, keyNum = keyNum) {
      override def map(value: T): Seq[Any] = {
        fun(value)
      }
    }).uid("jdbcBatchUpdate2")
  }

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   */
  def hbasePutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String,
                                                   batch: Int = 100,
                                                   flushInterval: Long = 3000,
                                                   keyNum: Int = KeyNum._1): DataStreamSink[_] = {
    this.hbasePutDS2[E](tableName, batch, flushInterval, keyNum) {
      value => {
        value.asInstanceOf[E]
      }
    }.uid("hbasePutDS")
  }

  /**
   * hbase批量sink操作，DataStream[T]中的T必须是HBaseBaseBean的子类
   *
   * @param tableName
   * hbase表名
   * @param batch
   * 每次sink最大的记录数
   * @param flushInterval
   * 多久flush一次（毫秒）
   * @param keyNum
   * 配置文件中的key后缀
   * @param fun
   * 将dstream中的数据映射为该sink组件所能处理的数据
   */
  def hbasePutDS2[E <: HBaseBaseBean[E] : ClassTag](tableName: String,
                                                    batch: Int = 100,
                                                    flushInterval: Long = 3000,
                                                    keyNum: Int = KeyNum._1)(fun: T => E): DataStreamSink[_] = {
    HBaseConnector.checkClass[E]()
    this.stream.addSink(new HBaseSink[T, E](tableName, batch, flushInterval, keyNum) {
      /**
       * 将数据构建成sink的格式
       */
      override def map(value: T): E = fun(value)
    }).uid("hbasePutDS2")
  }

  /**
   * 将消息发送到指定的消息队列中
   *
   * @param url
   * 消息队列的url
   * @param topic
   * 发送消息到指定的主题
   * @param mqType
   * 消息队列的类型，目前支持kafka与rocketmq
   * @param params
   * 额外的producer参数
   * @param keyNum
   * 指定配置的keyNum，可从配置或注解中获取对应配置信息
   */
  def sinkMQ[E <: MQRecord : ClassTag](params: Map[String, Object] = null,
                                          url: String = null,
                                          topic: String = null,
                                          tag: String = "*",
                                          mqType: MQType = MQType.kafka,
                                          batch: Int = 100, flushInterval: Long = 1000,
                                          keyNum: Int = KeyNum._1): DataStreamSink[_] = {
    this.sinkMQFun[E](params, url, topic, tag, mqType, batch, flushInterval, keyNum)(_.asInstanceOf[E])
  }

  /**
   * 将消息发送到指定的消息队列中
   *
   * @param url
   * 消息队列的url
   * @param topic
   * 发送消息到指定的主题
   * @param mqType
   * 消息队列的类型，目前支持kafka与rocketmq
   * @param params
   * 额外的producer参数
   * @param keyNum
   * 指定配置的keyNum，可从配置或注解中获取对应配置信息
   */
  def sinkMQFun[E <: MQRecord : ClassTag](params: Map[String, Object] = null,
                                       url: String = null,
                                       topic: String = null,
                                       tag: String = "*",
                                       mqType: MQType = MQType.kafka,
                                       batch: Int = 100, flushInterval: Long = 1000,
                                       keyNum: Int = KeyNum._1)(mapFunction: T => E): DataStreamSink[_] = {
    if (mqType == MQType.rocketmq) {
      this.sinkRocketMQFun[E](params, url, topic, tag, batch, flushInterval, keyNum)(mapFunction)
    } else {
      this.sinkKafkaFun[E](params, url, topic, batch, flushInterval, keyNum)(mapFunction)
    }
  }

  /**
   * 将数据实时sink到指定的kafka topic
   *
   * @param params
   * 额外的producer参数
   * @param url
   * 消息队列的url
   * @param topic
   * 发送消息到指定的主题
   * @param keyNum
   * 指定配置的keyNum，可从配置或注解中获取对应配置信息
   */
  def sinkKafka[E <: MQRecord : ClassTag](params: Map[String, Object] = null,
                                             url: String = null, topic: String = null,
                                             batch: Int = 100, flushInterval: Long = 1000,
                                             keyNum: Int = KeyNum._1): DataStreamSink[_] = {

    this.sinkKafkaFun[E](params, url, topic, batch, flushInterval, keyNum)(_.asInstanceOf[E]).uid("sinkKafka")
  }

  /**
   * 将数据实时sink到指定的kafka topic
   *
   * @param params
   * 额外的producer参数
   * @param url
   * 消息队列的url
   * @param topic
   * 发送消息到指定的主题
   * @param keyNum
   * 指定配置的keyNum，可从配置或注解中获取对应配置信息
   */
  def sinkKafkaFun[E <: MQRecord : ClassTag](params: Map[String, Object] = null,
                              url: String = null, topic: String = null,
                              batch: Int = 100, flushInterval: Long = 1000,
                              keyNum: Int = KeyNum._1)(fun: T => E): DataStreamSink[_] = {
    val finalBatch = if (FireKafkaConf.kafkaSinkBatch(keyNum) > 0) FireKafkaConf.kafkaSinkBatch(keyNum) else batch
    val finalInterval = if (FireKafkaConf.kafkaFlushInterval(keyNum) > 0) FireKafkaConf.kafkaFlushInterval(keyNum) else flushInterval

    this.stream.addSink(new KafkaSink[T, E](params, url, topic, finalBatch, finalInterval, keyNum) {
      override def map(value: T): E = fun(value)
    }).uid("sinkKafkaFun")
  }

  /**
   * 将数据实时sink到指定的rocketmq topic
   *
   * @param params
   * 额外的producer参数
   * @param url
   * 消息队列的url
   * @param topic
   * 发送消息到指定的主题
   * @param keyNum
   * 指定配置的keyNum，可从配置或注解中获取对应配置信息
   */
  def sinkRocketMQ[E <: MQRecord : ClassTag](params: Map[String, Object] = null,
                                             url: String = null, topic: String = null, tag: String = "*",
                                             batch: Int = 100, flushInterval: Long = 1000,
                                             keyNum: Int = KeyNum._1): DataStreamSink[_] = {
    this.sinkRocketMQFun[E](params, url, topic, tag, batch, flushInterval, keyNum)(_.asInstanceOf[E]).uid("sinkRocketMQ")
  }

  /**
   * 将数据实时sink到指定的rocketmq topic
   *
   * @param params
   * 额外的producer参数
   * @param url
   * 消息队列的url
   * @param topic
   * 发送消息到指定的主题
   * @param keyNum
   * 指定配置的keyNum，可从配置或注解中获取对应配置信息
   */
  def sinkRocketMQFun[E <: MQRecord : ClassTag](params: Map[String, Object] = null,
                                          url: String = null, topic: String = null, tag: String = "*",
                                          batch: Int = 100, flushInterval: Long = 1000,
                                          keyNum: Int = KeyNum._1)(fun: T => E): DataStreamSink[_] = {
    val finalBatch = if (FireRocketMQConf.rocketSinkBatch(keyNum) > 0) FireRocketMQConf.rocketSinkBatch(keyNum) else batch
    val finalInterval = if (FireRocketMQConf.rocketSinkFlushInterval(keyNum) > 0) FireRocketMQConf.rocketSinkFlushInterval(keyNum) else flushInterval

    this.stream.addSink(new RocketMQSink[T, E](params, url, topic, tag, finalBatch, finalInterval, keyNum) {
      override def map(value: T): E = fun(value)
    }).uid("sinkRocketMQFun")
  }

  /**
   * 将数据写入到kafka中
   */
  def sinkKafkaString[T <: String](kafkaParams: Map[String, Object] = null,
                             topic: String = null,
                             serializationSchema: SerializationSchema[String] = new SimpleStringSchema,
                             customPartitioner: FlinkKafkaPartitioner[String] = null,
                             semantic: Semantic = Semantic.AT_LEAST_ONCE,
                             kafkaProducersPoolSize: Int = FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE,
                             keyNum: Int = KeyNum._1): DataStreamSink[_] = {


    // 1. 设置producer相关额外参数
    val finalProducerConfig = KafkaUtils.getKafkaParams(kafkaParams, keyNum)

    // 2. 获取topic
    val finalTopic = KafkaUtils.getTopic(topic, keyNum)
    requireNonEmpty(finalTopic)(s"Topic不能为空，请检查keyNum=${keyNum}对应的配置信息")

    // 3. 获取kafka集群url
    val finalBrokers = KafkaUtils.getBrokers(finalProducerConfig, keyNum)
    requireNonEmpty(finalBrokers)(s"kafka broker地址不能为空，请检查keyNum=${keyNum}对应的配置信息")

    logDebug(
      s"""
         |--------> Sink kafka information. keyNum=$keyNum <--------
         |broker: $finalBrokers
         |topic: $finalTopic
         |""".stripMargin)

    // sink kafka埋点信息
    KafkaConnector.addDatasource(Datasource.KAFKA, finalBrokers, finalTopic, "", Operation.SINK)

    val kafkaProducer = new FlinkKafkaProducer[String](
      finalTopic,
      serializationSchema,
      finalProducerConfig,
      customPartitioner,
      semantic,
      kafkaProducersPoolSize
    )

    this.stream.asInstanceOf[DataStream[String]].addSink(kafkaProducer).uid("sinkKafkaString")
  }

  /**
   * 将数据写入到kafka中，开启仅一次的语义
   */
  def sinkKafkaExactlyOnce[T <: String](kafkaParams: Map[String, Object] = null,
                                        topic: String = null,
                                        serializationSchema: SerializationSchema[String] = new SimpleStringSchema,
                                        customPartitioner: FlinkKafkaPartitioner[String] = null,
                                        kafkaProducersPoolSize: Int = FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE,
                                        keyNum: Int = KeyNum._1): DataStreamSink[_] = {
    this.sinkKafkaString[T](kafkaParams, topic, serializationSchema, customPartitioner, Semantic.EXACTLY_ONCE, kafkaProducersPoolSize, keyNum)
  }

  /**
   * 将数据流实时写入jdbc数据源
   *
   * @param sql
   * dml sql语句
   * @param fields
   * 当为空时fire框架会自动解析sql中占位符对应的字段名称，如果解析失败，用户可手动指定
   * @param autoConvert
   * 是否自动匹配sql中声明的带有下划线的字段与JavaBean中声明的驼峰标识的成员变量
   * @param keyNum
   * 数据源配置索引
   */
  def sinkJdbc(sql: String, fields: Seq[String] = null, autoConvert: Boolean = true, keyNum: Int = 1): DataStreamSink[T] = {
    val (jdbcConf: JdbcConf, connectionTimeout: Int, columnList: Seq[String]) = this.preparedJdbcSinkParam(sql, fields, autoConvert, keyNum)

    val connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
      .withUrl(jdbcConf.url)
      .withDriverName(jdbcConf.driverClass)
      .withUsername(jdbcConf.username)
      .withPassword(jdbcConf.password)

    // 此处反射调用withConnectionCheckTimeoutSeconds方法是为了兼容不同的Flink版本
    // Flink1.12没有withConnectionCheckTimeoutSeconds这个方法，通过反射验证
    val checkTimeoutMethod = ReflectionUtils.getMethodByName(classOf[JdbcConnectionOptionsBuilder], "withConnectionCheckTimeoutSeconds")
    if (checkTimeoutMethod != null) {
      // checkTimeoutMethod.invoke(connOptions, connectionTimeout)
      connOptions.withConnectionCheckTimeoutSeconds(connectionTimeout)
    }

    // 将流式数据实时写入到指定的关系型数据源中
    import org.apache.flink.connector.jdbc.JdbcSink
    stream.addSink(JdbcSink.sink(sql,
      new JdbcStatementBuilder[T]() {
        override def accept(stat: PreparedStatement, bean: T): Unit = {
          DBUtils.setPreparedStatement(columnList, stat, bean)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(FireJdbcConf.batchSize(keyNum))
        .withBatchIntervalMs(FireJdbcConf.jdbcFlushInterval(keyNum))
        .withMaxRetries(FireJdbcConf.maxRetry(keyNum).toInt)
        .build(),
      connOptions.build()
    )).uid("sinkJdbc")
  }

  /**
   * 将数据流实时写入jdbc数据源
   *
   * @param sql
   * dml sql语句
   * @param fields
   * 当为空时fire框架会自动解析sql中占位符对应的字段名称，如果解析失败，用户可手动指定
   * @param autoConvert
   * 是否自动匹配sql中声明的带有下划线的字段与JavaBean中声明的驼峰标识的成员变量
   * @param keyNum
   * 数据源配置索引
   */
  def sinkJdbcExactlyOnce(sql: String, fields: Seq[String] = null, autoConvert: Boolean = true,
                          dbType: Datasource = Datasource.MYSQL,
                          transactionPerConnection: Boolean = true, maxCommitAttempts: Int = 3,
                          recoveredAndRollback: Boolean = true, allowOutOfOrderCommits: Boolean = false,
                          keyNum: Int = 1): DataStreamSink[T] = {
    val (jdbcConf: JdbcConf, connectionTimeout: Int, columns: Seq[String]) = this.preparedJdbcSinkParam(sql, fields, autoConvert, keyNum)

    val isExists = ReflectionUtils.existsClass("org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions")
    if (!isExists) throw new RuntimeException("低于Flink1.13版本暂不支持sinkJdbcExactlyOnce！")

    // 为了兼容Flink1.13，使用反射判断withTransactionPerConnection方法是否存在
    val jdbcExactlyOnceOptionsBuilder = JdbcExactlyOnceOptions.builder()
      .withMaxCommitAttempts(maxCommitAttempts)
      .withRecoveredAndRollback(recoveredAndRollback)
      .withAllowOutOfOrderCommits(allowOutOfOrderCommits)
    val method = ReflectionUtils.getMethodByName(classOf[JDBCExactlyOnceOptionsBuilder], "withTransactionPerConnection")
    if (method != null) jdbcExactlyOnceOptionsBuilder.withTransactionPerConnection(transactionPerConnection)

    // 将流式数据实时写入到指定的关系型数据源中
    import org.apache.flink.connector.jdbc.JdbcSink
    stream.addSink(JdbcSink.exactlyOnceSink(sql,
      new JdbcStatementBuilder[T]() {
        override def accept(stat: PreparedStatement, bean: T): Unit = {
          DBUtils.setPreparedStatement(columns, stat, bean)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(FireJdbcConf.batchSize(keyNum))
        .withBatchIntervalMs(FireJdbcConf.jdbcFlushInterval(keyNum))
        .withMaxRetries(0)
        .build(),
      jdbcExactlyOnceOptionsBuilder.build(),
      new SerializableSupplier[XADataSource]() {
        override def get(): XADataSource = DBUtils.buildXADataSource(jdbcConf, dbType)
      }
    )).uid("sinkJdbcExactlyOnce")
  }

  /**
   * 工具方法，准备jdbc sink所需相关配置与参数
   */
  @Internal
  private[this] def preparedJdbcSinkParam(sql: JString, params: Seq[JString], autoConvert: Boolean, keyNum: Int): (JdbcConf, Int, Seq[JString]) = {
    requireNonEmpty(sql)("SQL语句不能为空！")

    // 1. 获取配置文件中数据源信息
    val jdbcConf = JdbcConf(keyNum)
    val connectionTimeout = FireJdbcConf.jdbcConnectionTimeout(keyNum)
    requireNonEmpty(jdbcConf.url, jdbcConf.driverClass, jdbcConf.username)(s"keyNum=${keyNum} 对应的jdbc相关数据源信息非法，请检查！")

    logInfo(
      s"""
         |--------> Sink jdbc information. keyNum=$keyNum <--------
         |url: ${jdbcConf.url}
         |driver: ${jdbcConf.driverClass}
         |username: ${jdbcConf.username}
         |connectionTimeout: $connectionTimeout
         |""".stripMargin)

    // 2. 获取SQL中的占位符字段列表，如果主动指定，则基于指定的字段列表，否则使用自动推断
    val columns = if (noEmpty(params)) params else SQLUtils.parsePlaceholder(sql).map(column => if (autoConvert) column.toHump else column)
    logInfo(s"Sink jdbc columns: $columns")

    // 3. sink jdbc埋点信息
    // TODO: 解析sql血缘关系，包括针对表做了哪些操作，数据的流转等
    LineageManager.addDBSql(Datasource.parse(DBUtils.dbTypeParser(jdbcConf.driverClass, jdbcConf.url)), jdbcConf.url, jdbcConf.username, sql, Operation.UPDATE)
    (jdbcConf, connectionTimeout, columns)
  }
}
