package com.zto.bigdata.spark.common.ext

import com.alibaba.fastjson.JSON
import com.zto.bigdata.spark.common.bean.RestartParams
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.rest.RestCase
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{ColumnName, DataFrame, Encoders}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.{Request, Response}

/**
  * 实时平台Spark通用父类
  * Created by ChengLong on 2018-03-28.
  */
trait BaseSparkStreaming extends BaseSpark {
  var checkPointDir: String = _
  var batchDuration: Long = _
  var externalConf: RestartParams = _

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param batchDuration
    * Streaming每个批次间隔时间
    * @param isCheckPoint
    * 是否做checkpoint
    */
  def init(batchDuration: Long, isCheckPoint: Boolean): Unit = {
    this.init(batchDuration, isCheckPoint, null)
  }

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param batchDuration
    * Streaming每个批次间隔时间
    * @param isCheckPoint
    * 是否做checkpoint
    * @param conf
    * 传入自己构建的sparkConf对象，可以为空
    */
  def init(batchDuration: Long, isCheckPoint: Boolean, conf: SparkConf): Unit = {
    val tmpConf = buildConf(conf)
    if (this.sc == null) {
      // 添加streaming相关的restful接口，并启动
      this.restfulRegister.addRest(RestCase("get", "/system/restartStreaming", this.restartStreaming)).startRestServer
      this.init(tmpConf)
    }
    this.batchDuration = batchDuration
    if (!isCheckPoint) {
      if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
        // 重启SparkContext对象
        this.ssc = new StreamingContext(tmpConf, Seconds(Math.abs(batchDuration)))
        this.sc = this.ssc.sparkContext
      } else {
        this.ssc = new StreamingContext(this.sc, Seconds(Math.abs(batchDuration)))
      }
      this.ssc.remember(Seconds(Math.abs(batchDuration) * 10))
      this.process
    } else {
      this.checkPointDir = GlobalConstants.SparkConf.chkPointDirPrefix + this.appName
      this.ssc = StreamingContext.getOrCreate(this.checkPointDir, createStreamingContext _)

      // 初始化Streaming
      def createStreamingContext(): StreamingContext = {
        tmpConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
        if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
          // 重启SparkContext对象
          this.ssc = new StreamingContext(tmpConf, Seconds(Math.abs(batchDuration)))
          this.sc = this.ssc.sparkContext
        } else {
          this.ssc = new StreamingContext(this.sc, Seconds(Math.abs(batchDuration)))
        }
        this.ssc.checkpoint(checkPointDir)
        this.process
        this.ssc
      }
    }
    this.conf = tmpConf
  }

  /**
    * 构建内部使用的SparkConf对象
    */
  override def buildConf(conf: SparkConf = null): SparkConf = {
    val tmpConf = if (conf == null) {
      new SparkConf()
        .setAppName(this.appName)
        // 开启后可能会导致streaming不稳定
        // .set("spark.speculation", "true")
        .set("spark.port.maxRetries", "200")
        .set("spark.ui.retainedJobs", "500")
        .set("spark.ui.retailedStages", "300")
        .set("spark.default.parallelism", "300")
        .set("spark.sql.broadcastTimeout", "3000")
        .set("spark.storage.memoryFraction", "0.4")
        .set("spark.streaming.concurrentJobs", "1")
        .set("spark.ui.timeline.tasks.maximum", "300")
        .set("spark.sql.parquet.writeLegacyFormat", "true")
        .set("spark.streaming.backpressure.enabled", "true")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")
        // 解决cluster模式下不稳定的问题
        .set("spark.streaming.kafka.consumer.cache.enabled", "false")
        .set("spark.streaming.kafka.maxRatePerPartition", "100") // 每个批次从每个partition中每秒中最大拉取的数据量
        .set("hive.metastore.uris", GlobalConstants.HiveConf.metaStoreUris)
    } else conf

    // 若重启SparkContext对象，则设置restful传递过来的新的配置信息
    if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
      if (this.externalConf.getSparkConf != null && this.externalConf.getSparkConf.size() > 0) {
        tmpConf.setAll(this.externalConf.getSparkConf.toScalaMap)
      }
    }

    tmpConf
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用，若需使用
    * checkpoint中的数据，则子类必须复写该方法
    */
  override def process: Unit = {
    try {
      ParamUtils.requireNonNull(this.checkPointDir, "当开启checkPoint机制后，必须复写父类的process方法")
      ParamUtils.requireNonNull(this.externalConf, "当需要热重启功能时，必须将对接kafka的代码写在process方法内")
    } finally {
      this.destory
    }
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
    * 用于重置StreamingContext（仅支持batch时间的修改）
    *
    * @param request
    * @param response
    * @return
    */
  def restartStreaming(request: Request, response: Response): AnyRef = {
    // val param = request.queryString()
    val param = if (StringUtils.isNotBlank(request.queryString()))
      """
        | {"batchDuration":10,"restartSparkContext":false,"stopGracefully": false,"sparkConf":{"spark.streaming.concurrentJobs":"2"}}
      """.stripMargin
    else
      """
        | {"batchDuration":20,"restartSparkContext":true,"stopGracefully": false,"sparkConf":{"spark.streaming.concurrentJobs":"2"}}
      """.stripMargin
    if (StringUtils.isNotBlank(param)) {
      this.externalConf = JSON.parseObject(param, classOf[RestartParams])
      this.ssc.stop(this.externalConf.isRestartSparkContext, this.externalConf.isStopGracefully)
      this.init(this.externalConf.getBatchDuration, false)
    }
    GlobalConstants.Status.SUCCESS
  }

}
