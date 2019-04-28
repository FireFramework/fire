package com.zto.bigdata.spark.common.ext

import java.util.Properties

import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.rest.RestCase
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.{Request, Response}

/**
  * 实时平台Spark通用父类
  * Created by ChengLong on 2018-03-28.
  */
trait BaseSparkStreaming extends BaseSpark {
  var checkPointDir: String = _
  var batchDuration: Long = _

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param batchDuration
    * Streaming每个批次间隔时间
    */
  def init(batchDuration: Long, checkPoint: Boolean): Unit = {
    val tmpConf = buildConf(this.appName)
    if (checkPoint) {
      tmpConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    }
    if (this.sc == null) {
      this.init(this.appName, tmpConf)
      this.restfulRegister.addRest(RestCase("get", "/system/restartStreaming", this.restartStreaming))
    }
    this.batchDuration = batchDuration
    if (!checkPoint) {
      this.ssc = new StreamingContext(this.sc, Seconds(Math.abs(batchDuration)))
      this.ssc.remember(Seconds(Math.abs(batchDuration) * 10))
      this.process
    } else {
      this.checkPointDir = GlobalConstants.SparkConf.chkPointDirPrefix + this.appName
      this.ssc = StreamingContext.getOrCreate(this.checkPointDir, createStreamingContext _)

      // 初始化Streaming
      def createStreamingContext(): StreamingContext = {
        this.ssc = new StreamingContext(this.sc, Seconds(Math.abs(batchDuration)))
        this.ssc.checkpoint(checkPointDir)
        this.process
        this.ssc
      }
    }
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用，若需使用
    * checkpoint中的数据，则子类必须复写该方法
    */
  override def process: Unit = {
    if (StringUtils.isNotBlank(this.checkPointDir)) throw new IllegalArgumentException(GlobalConstants.PrintModule.REAL_TIME_PROCESS_METHOD)
  }

  /**
    * 构建内部使用的SparkConf对象
    */
  private def buildConf(appName: String): SparkConf = {
    val tmpAppName = if (StringUtils.isBlank(appName)) this.appName else appName
    new SparkConf()
      .setAppName(tmpAppName)
      .set("spark.speculation", "true")
      .set("spark.port.maxRetries", "200")
      .set("spark.ui.retainedJobs", "500")
      .set("spark.ui.retailedStages", "300")
      .set("spark.default.parallelism", "300")
      .set("spark.sql.broadcastTimeout", "3000")
      .set("spark.storage.memoryFraction", "0.4")
      // .set("spark.streaming.concurrentJobs", "2")
      .set("spark.ui.timeline.tasks.maximum", "300")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      // .set("spark.streaming.kafka.maxRatePerPartition", "10000") // 每个批次从每个partition中每秒中最大拉取的数据量
      .set("spark.sql.parquet.writeLegacyFormat", "true")
      .set("hive.metastore.uris", GlobalConstants.HiveConf.metaStoreUris)
  }

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param appName
    * job名称，默认为类名称
    * @param conf
    * SparkConf配置信息
    */
  override def init(appName: String = "", conf: SparkConf = null): Unit = {
    if (conf == null) {
      this.conf = this.buildConf(appName)
    } else {
      this.conf = conf
    }
    if (SystemInfoUtils.isWindows) {
      this.spark = SparkSession.builder().config(this.conf).master("local[*]").enableHiveSupport().getOrCreate()
    } else {
      this.spark = SparkSession.builder().config(this.conf).enableHiveSupport().getOrCreateCarbonSession
    }
    this.spark.registerAll()
    this.sc = this.spark.sparkContext
    this.sc.setLogLevel(GlobalConstants.SparkConf.logLevel)
    this.sc.addSparkListener(new BaseSparkListener(this))
    this.hiveContext = this.spark.sqlContext
    this.sqlContext = this.hiveContext
    this.hbaseContext = SingletonFactory.getHBaseContextInstance(sc)
    this.applicationId = SparkUtils.getApplicationId(this.spark)
    this.webUI = SparkUtils.getWebUI(this.spark)
  }

  /**
    * 解析DStream中每个rdd的json数据，并转为DataFrame类型
    *
    * @param rdd
    * DStream中的每个rdd
    * @param schema
    * 目标DataFrame类型的schema
    * @param requireBefore
    * 是否需要before信息
    * @return
    */
  def parseJson2DataFrameV(rdd: RDD[String], schema: Class[_], requireBefore: Boolean = false): DataFrame = {
    val ds = this.spark.createDataset(rdd)(Encoders.STRING)
    val df = ds.select(from_json(new ColumnName("value"), SparkUtils.buildSchema2Kafka(schema)).as("data"))
    if (requireBefore)
      df.select("data.*")
    else
      df.select("data.after.*")
  }

  /**
    * 解析DStream中每个rdd的json数据，并转为DataFrame类型
    *
    * @param rdd
    * DStream中的每个rdd
    * @param schema
    * 目标DataFrame类型的schema
    * @param requireBefore
    * 是否需要before信息
    * @return
    */
  def parseJson2DataFrame(rdd: RDD[ConsumerRecord[String, String]], schema: Class[_], requireBefore: Boolean = false): DataFrame = {
    val ds = this.spark.createDataset(rdd.map(t => t.value()))(Encoders.STRING)
    val df = ds.select(from_json(new ColumnName("value"), SparkUtils.buildSchema2Kafka(schema)).as("data"))
    if (requireBefore)
      df.select("data.*")
    else
      df.select("data.after.*")
  }

  /**
    * kafka配置信息
    *
    * @param groupId
    * 消费组
    * @param offset
    * offset位点，smallest、largest，默认为largest
    * @return
    * kafka相关配置
    */
  def kafkaParams(groupId: String = GlobalConstants.SparkConf.kafkaGroupId, kafkaBrokers: String = GlobalConstants.SparkConf.kafkaBrokers, offset: String = GlobalConstants.KafkaConf.offsetLargest, commit: Boolean = GlobalConstants.SparkConf.kafkaEnableAutoCommit): Map[String, Object] = {
    // 如果配置文件中没有指定spark.kafka.group.id，则默认为appName
    val kafkaGroupId = if (StringUtils.isNotBlank(groupId)) groupId else this.appName
    SparkUtils.kafkaParams(kafkaGroupId, kafkaBrokers, offset)
  }

  /**
    * kafka相关配置
    */
  val kafkaProperties = new Properties();
  {
    kafkaProperties.put("zookeeper.connect", GlobalConstants.zkUrl) // 声明zk
    kafkaProperties.put("key.serializer.class", "kafka.serializer.StringEncoder")
    kafkaProperties.put("acks", "all")
    kafkaProperties.put("retries", "3")
    kafkaProperties.put("serializer.class", "kafka.serializer.StringEncoder")
    kafkaProperties.put("metadata.broker.list", GlobalConstants.SparkConf.kafkaBrokers) // 声明kafka
  }

  /**
    * 用于重置StreamingContext
    *
    * @param request
    * @param response
    * @return
    */
  def restartStreaming(request: Request, response: Response): AnyRef = {
    val param = request.queryString()
    this.batchDuration = if (StringUtils.isNotBlank(param)) param.toLong else this.batchDuration
    this.ssc.stop(false, false)
    this.init(this.batchDuration, false)
    GlobalConstants.Status.SUCCESS
  }

}
