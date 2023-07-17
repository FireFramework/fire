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

package com.zto.fire.spark.ext.core

import com.zto.fire._
import com.zto.fire.common.enu.{Datasource, Operation => FOperation}
import com.zto.fire.common.bean.Generator
import com.zto.fire.common.conf.{FireKafkaConf, FireRocketMQConf, KeyNum}
import com.zto.fire.common.util.{LineageManager, MQType, OSUtils}
import com.zto.fire.common.util.MQType.MQType
import com.zto.fire.core.Api
import com.zto.fire.hudi.conf.FireHudiConf
import com.zto.fire.jdbc.JdbcConnectorBridge
import com.zto.fire.spark.conf.FireSparkConf
import com.zto.fire.spark.connector.SparkConnectors._
import com.zto.fire.spark.ext.provider._
import com.zto.fire.spark.sql.SparkSqlUtils
import com.zto.fire.spark.util.{SparkSingletonFactory, SparkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{ConsumerStrategy, LocationStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

import java.io.InputStream
import scala.reflect.ClassTag

/**
 * SparkContext扩展
 *
 * @param spark
 * sparkSession对象
 * @author ChengLong 2019-5-18 10:51:19
 */
class SparkSessionExt(_spark: SparkSession) extends Api with JdbcConnectorBridge with JdbcSparkProvider
  with HBaseBulkProvider with SqlProvider with HBaseConnectorProvider with HBaseHadoopProvider with KafkaSparkProvider {
  private[fire] lazy val ssc = SparkSingletonFactory.getStreamingContext
  private[this] lazy val appName = ssc.sparkContext.appName

  /**
   * 根据给定的集合，创建rdd
   *
   * @param seq
   * seq
   * @param numSlices
   * 分区数
   * @return
   * RDD
   */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
    this.sc.parallelize(seq, numSlices)
  }

  /**
   * 根据给定的集合，创建rdd
   *
   * @param seq
   * seq
   * @param numSlices
   * 分区数
   * @return
   * RDD
   */
  def createRDD[T: ClassTag](seq: Seq[T], numSlices: Int = sc.defaultParallelism): RDD[T] = {
    this.parallelize[T](seq, numSlices)
  }

  /**
   * 创建socket流
   */
  def createSocketStream[T: ClassTag](
                                       hostname: String,
                                       port: Int,
                                       converter: (InputStream) => Iterator[T],
                                       storageLevel: StorageLevel
                                     ): ReceiverInputDStream[T] = {
    this.ssc.socketStream[T](hostname, port, converter, storageLevel)
  }

  /**
   * 创建socket文本流
   */
  def createSocketTextStream(
                              hostname: String,
                              port: Int,
                              storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                            ): ReceiverInputDStream[String] = {
    this.ssc.socketTextStream(hostname, port, storageLevel)
  }


  /**
   * 构建Kafka DStream流
   *
   * @param kafkaParams
   * kafka参数
   * @param topics
   * topic列表
   * @return
   * DStream
   */
  def createKafkaDirectStream(kafkaParams: Map[String, Object] = null, topics: Set[String] = null, groupId: String = null, keyNum: Int = KeyNum._1): DStream[ConsumerRecord[String, String]] = {
    this.ssc.createDirectStream(kafkaParams, topics, groupId, keyNum)
  }

  /**
   * 构建RocketMQ拉取消息的DStream流
   *
   * @param rocketParam
   * rocketMQ相关消费参数
   * @param groupId
   * groupId
   * @param topics
   * topic列表
   * @param consumerStrategy
   * 从何处开始消费
   * @return
   * rocketMQ DStream
   */
  def createRocketMqPullStream(rocketParam: JMap[String, String] = null,
                               groupId: String = this.appName,
                               topics: String = null,
                               tag: String = null,
                               consumerStrategy: ConsumerStrategy = ConsumerStrategy.lastest,
                               locationStrategy: LocationStrategy = LocationStrategy.PreferConsistent,
                               instance: String = "",
                               keyNum: Int = KeyNum._1): InputDStream[MessageExt] = {
    this.ssc.createRocketPullStream(rocketParam, groupId, topics, tag, consumerStrategy, locationStrategy, instance, keyNum)
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
  def createMQStream(processFun: RDD[String] => Unit, mqType: MQType = MQType.auto, keyNum: Int = KeyNum._1)(implicit reTry: Int = 3, duration: Long = 3000, autoCommit: Boolean = true, exitOnFailure: Boolean = true): Unit = {
    def kafkaStream: Unit = this.createKafkaDirectStream(keyNum = keyNum).foreachRDDAtLeastOnce(rdd => processFun(rdd.map(t => t.value())))(reTry, duration, autoCommit, exitOnFailure)

    def rocketStream: Unit = this.createRocketMqPullStream(keyNum = keyNum).foreachRDDAtLeastOnce(rdd => processFun(rdd.map(t => new String(t.getBody))))(reTry, duration, autoCommit, exitOnFailure)

    mqType match {
      case MQType.kafka => kafkaStream
      case MQType.rocketmq => rocketStream

      case _ => {
        val kafkaTopicValue = FireKafkaConf.kafkaBrokers(keyNum)
        val rocketTopicValue = FireRocketMQConf.rocketNameServer(keyNum)

        // 根据配置文件区分不同的消费场景（消费kafka还是rocketmq）
        if (noEmpty(kafkaTopicValue) && noEmpty(rocketTopicValue)) {
          throw new IllegalArgumentException(s"kafka和rocketmq对应的连接参数同时指定，自动推断失败！keyNum=${keyNum}")
        }

        if (isEmpty(kafkaTopicValue) && isEmpty(rocketTopicValue)) {
          throw new IllegalArgumentException(s"kafka和rocketmq对应的连接参数均未被指定，自动推断失败！keyNum=${keyNum}")
        }

        if (noEmpty(kafkaTopicValue)) kafkaStream else rocketStream
      }
    }
  }

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
  def createGenStream[T: ClassTag](gen: => T, qps: Long = 1000): ReceiverInputDStream[T] = {
    this.receiverStream[T](new GenConnector(gen, qps))
  }

  /**
   * 创建Int型数据随机数DStream
   *
   * @param qps
   * 数据生成的qps
   * @return
   * ReceiverInputDStream[T]
   */
  def createRandomIntStream(qps: Long = 1000): ReceiverInputDStream[Int] = {
    this.receiverStream[Int](new RandomIntConnector(qps))
  }

  /**
   * 创建Long型数据随机数DStream
   *
   * @param qps
   * 数据生成的qps
   * @return
   * ReceiverInputDStream[T]
   */
  def createRandomLongStream(qps: Long = 1000): ReceiverInputDStream[Long] = {
    this.receiverStream[Long](new RandomLongConnector(qps))
  }

  /**
   * 创建Double型数据随机数DStream
   *
   * @param qps
   * 数据生成的qps
   * @return
   * ReceiverInputDStream[T]
   */
  def createRandomDoubleStream(qps: Long = 1000): ReceiverInputDStream[Double] = {
    this.receiverStream[Double](new RandomDoubleConnector(qps))
  }

  /**
   * 创建Float型数据随机数DStream
   *
   * @param qps
   * 数据生成的qps
   * @return
   * ReceiverInputDStream[T]
   */
  def createRandomFloatStream(qps: Long = 1000): ReceiverInputDStream[Float] = {
    this.receiverStream[Float](new RandomFloatConnector(qps))
  }

  /**
   * 创建根据指定规则生成对象实例的DataGenReceiver
   *
   * @param qps
   * 数据生成的qps
   * @return
   * ReceiverInputDStream[T]
   */
  def createUUIDStream(qps: Long = 1000): ReceiverInputDStream[String] = {
    this.receiverStream[String](new UUIDConnector(qps))
  }

  /**
   * 创建JavaBean DStream流，JavaBean必须实现Generator接口
   *
   * @param qps
   * 数据生成的qps
   * @tparam T
   * 生成数据的类型
   * @return
   * ReceiverInputDStream[T]
   */
  def createBeanStream[T <: Generator[T] : ClassTag](qps: Long = 1000): ReceiverInputDStream[T] = {
    this.receiverStream[T](new BeanConnector[T](qps))
  }

  /**
   * 创建基于JavaBean序列化JSON DStream流，JavaBean必须实现Generator接口
   *
   * @param qps
   * 数据生成的qps
   * @tparam T
   * 生成数据的类型
   * @return
   * ReceiverInputDStream[T]
   */
  def createJSONStream[T <: Generator[T] : ClassTag](qps: Long = 1000): ReceiverInputDStream[String] = {
    this.receiverStream(new JSONConnector[T](100))
  }

  /**
   * 接受自定义receiver的数据
   *
   * @param receiver
   * 自定义receiver
   * @tparam T
   * 接受的数据类型
   * @return
   * 包装后的DStream[T]
   */
  def receiverStream[T: ClassTag](receiver: Receiver[T]): ReceiverInputDStream[T] = {
    LineageManager.addCustomizeDatasource("customize_source", OSUtils.getIp, receiver.getClass.getSimpleName, FOperation.SOURCE)
    this.ssc.receiverStream[T](receiver)
  }

  /**
   * 接受自定义receiver的数据
   *
   * @param receiver
   * 自定义receiver
   * @tparam T
   * 接受的数据类型
   * @return
   * 包装后的DStream[T]
   */
  def addSource[T: ClassTag](receiver: Receiver[T]): ReceiverInputDStream[T] = {
    this.ssc.receiverStream[T](receiver)
  }


  /**
   * 启动StreamingContext
   */
  override def start(): Unit = {
    if (this.ssc != null) {
      this.ssc.startAwaitTermination()
    }
  }

  /**
   * spark datasource read api增强，提供配置文件进行覆盖配置
   *
   * @param format
   * DataSource中的format
   * @param loadParams
   * load方法的参数，多个路径以逗号分隔
   * @param options
   * DataSource中的options，支持参数传入和配置文件读取，相同的选项配置文件优先级更高
   * @param keyNum
   * 用于标识不同DataSource api所对应的配置文件中key的后缀
   */
  def readEnhance(format: String = "",
                  loadParams: Seq[String] = null,
                  options: Map[String, String] = Map.empty,
                  keyNum: Int = KeyNum._1): Unit = {
    val finalFormat = if (noEmpty(FireSparkConf.datasourceFormat(keyNum))) FireSparkConf.datasourceFormat(keyNum) else format
    val finalLoadParam = if (noEmpty(FireSparkConf.datasourceLoadParam(keyNum))) FireSparkConf.datasourceLoadParam(keyNum).split(",").toSeq else loadParams
    this.logger.info(s"--> Spark DataSource read api参数信息（keyNum=$keyNum）<--")
    this.logger.info(s"format=${finalFormat} loadParams=${finalLoadParam}")

    requireNonEmpty(finalFormat, finalLoadParam)
    SparkSingletonFactory.getSparkSession.read.format(format).options(SparkUtils.optionsEnhance(options, keyNum)).load(finalLoadParam: _*)
  }

  /**
   * 将已hive表为载体的hudi表注册成hudi表
   *
   * @param hiveTableName
   * hive表名
   * @param hudiViewName
   * hudi视图名
   */
  def createOrReplaceHudiTempView(hiveTableName: String, hudiViewName: String): Unit = {
    requireNonEmpty(hiveTableName)("Hive表名不能为空！")
    requireNonEmpty(hudiViewName)("Hudi视图表名不能为空")

    if (!this.spark.catalog.tableExists(hiveTableName)) throw new IllegalArgumentException(s"Hive表名不存在：$hiveTableName")
    val df = this.spark.read.format(FireHudiConf.HUDI_FORMAT).load(SparkSqlUtils.getTablePath(hiveTableName))
    df.createOrReplaceTempView(hudiViewName)
  }
}