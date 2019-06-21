package com.zto.bigdata.spark.common.core

import com.alibaba.fastjson.JSON
import com.zto.bigdata.spark.common.anno.Rest
import com.zto.bigdata.spark.common.bean.RestartParams
import com.zto.bigdata.spark.common.bean.rest.ResultMsg
import com.zto.bigdata.spark.common.enu.ErrorCode
import com.zto.bigdata.spark.common.rest.{RequestMethod, RestCase}
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.{Request, Response}
import com.zto.bigdata.spark.common.ext.SparkExt._

import scala.collection.JavaConversions

/**
  * 实时平台Spark通用父类
  * Created by ChengLong on 2018-03-28.
  */
trait BaseSparkStreaming extends BaseSpark {
  var checkPointDir: String = _
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
      this.init(tmpConf)
      if (SystemInfoUtils.isLinux) {
        this.restfulRegister
          .addRest(RestCase(RequestMethod.POST.toString, "/system/restartStreaming", this.restartStreaming))
          .startRestServer
      }
    }
    // 判断是否为热重启，batchDuration优先级分别为 [ 代码<配置文件<热重启 ]
    this.batchDuration = SparkUtils.overrideBatchDuration(batchDuration, this.externalConf != null && this.externalConf.getBatchDuration != null)
    if (!isCheckPoint) {
      if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
        // 重启SparkContext对象
        this.ssc = new StreamingContext(tmpConf, Seconds(Math.abs(this.batchDuration)))
        this.sc = this.ssc.sparkContext
      } else {
        this.ssc = new StreamingContext(this.sc, Seconds(Math.abs(this.batchDuration)))
      }
      this.ssc.remember(Seconds(Math.abs(this.batchDuration) * 10))
      this.process
    } else {
      this.checkPointDir = GlobalConstants.SparkConf.chkPointDirPrefix + this.appName
      this.ssc = StreamingContext.getOrCreate(this.checkPointDir, createStreamingContext _)

      // 初始化Streaming
      def createStreamingContext(): StreamingContext = {
        tmpConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
        if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
          // 重启SparkContext对象
          this.ssc = new StreamingContext(tmpConf, Seconds(Math.abs(this.batchDuration)))
          this.sc = this.ssc.sparkContext
        } else {
          this.ssc = new StreamingContext(this.sc, Seconds(Math.abs(this.batchDuration)))
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
        .set("spark.ui.killEnabled", "false")
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
        // .set("spark.streaming.kafka.maxRatePerPartition", "100") // 每个批次从每个partition中每秒中最大拉取的数据量
        .set("spark.streaming.kafka.consumer.cache.enabled", "false")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("hive.metastore.uris", GlobalConstants.HiveConf.getMetastoreUrl)
    } else conf

    // 若重启SparkContext对象，则设置restful传递过来的新的配置信息
    if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
      if (this.externalConf.getSparkConf != null && this.externalConf.getSparkConf.size() > 0) {
        tmpConf.setAll(JavaConversions.mapAsScalaMap(this.externalConf.getSparkConf))
      }
    }

    tmpConf
  }

  /**
    * Streaming的处理过程强烈建议放到process中，保持风格统一
    * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
    * 1. 开启checkpoint
    * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
    */
  override def process: Unit = {
    ParamUtils.requireNull(this.checkPointDir, "当开启checkPoint机制时，必须将对接kafka的代码写在process方法内")
    ParamUtils.requireNull(this.externalConf, "当需要使用热重启功能时，必须将对接kafka的代码写在process方法内")
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
  def kafkaParams(groupId: String = null, kafkaBrokers: String = null, offset: String = null, autoCommit: Boolean = false, keyNum: Int = 1): Map[String, Object] = {
    // 如果配置文件中没有指定spark.kafka.group.id，则默认为appName
    val finalKafkaGroupId = if (StringUtils.isBlank(groupId)) {
      if (StringUtils.isNotBlank(GlobalConstants.KafkaConf.kafkaGroupId(keyNum))) {
        GlobalConstants.KafkaConf.kafkaGroupId(keyNum)
      } else {
        ssc.sparkContext.appName
      }
    } else {
      groupId
    }

    SparkUtils.kafkaParams(finalKafkaGroupId, kafkaBrokers, offset, autoCommit, keyNum)
  }

  /**
    * 用于重置StreamingContext（仅支持batch时间的修改）
    *
    * @return
    * 响应结果
    */
  @Rest("/system/restartStreaming")
  def restartStreaming(request: Request, response: Response): AnyRef = {
    val param = request.body()
    val msg = new ResultMsg()
    try {
      if (StringUtils.isNotBlank(param)) {
        this.externalConf = JSON.parseObject(param, classOf[RestartParams])
        this.ssc.stop(this.externalConf.isRestartSparkContext, this.externalConf.isStopGracefully)
        this.init(this.externalConf.getBatchDuration, this.externalConf.isCheckPoint)
      }
      msg.buildSuccess("重启StreamingContext成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.wrapLogError("重启StreamingContext失败：" + e.getMessage)
        msg.buildError(e.getMessage, ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

}
