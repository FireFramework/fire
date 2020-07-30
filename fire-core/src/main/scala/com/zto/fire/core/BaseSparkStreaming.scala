package com.zto.fire.core

import com.alibaba.fastjson.JSON
import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.RestartParams
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.conf.{FireFrameworkConf, FireKafkaConf, FireSparkConf}
import com.zto.fire.common.enu.{ErrorCode, JobType, RequestMethod}
import com.zto.fire.common.util.{KafkaUtils, PropUtils, ValueUtils}
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.rest.RestCase
import com.zto.fire.core.util.SparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import spark.{Request, Response}

import scala.collection.JavaConversions

/**
 * 实时平台Spark通用父类
 * Created by ChengLong on 2018-03-28.
 */
trait BaseSparkStreaming extends BaseSpark {
  var checkPointDir: String = _
  var externalConf: RestartParams = _
  override val jobType = JobType.SPARK_STREAMING

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
      this.restfulRegister
        .addRest(RestCase(RequestMethod.POST.toString, "/system/streaming/hotRestart", this.hotRestart))
        .startRestServer
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
      val rememberTime = FireFrameworkConf.streamingRemember
      if (rememberTime > 0) this.ssc.remember(Milliseconds(Math.abs(rememberTime)))
      this.process
    } else {
      this.checkPointDir = FireSparkConf.chkPointDirPrefix + this.appName
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
    this._conf = tmpConf
  }

  /**
   * 构建内部使用的SparkConf对象
   */
  override def buildConf(conf: SparkConf = null): SparkConf = {
    val tmpConf = super.buildConf(conf)

    // 若重启SparkContext对象，则设置restful传递过来的新的配置信息
    if (this.externalConf != null && this.externalConf.isRestartSparkContext) {
      if (this.externalConf.getSparkConf != null && this.externalConf.getSparkConf.size() > 0) {
        tmpConf.setAll(JavaConversions.mapAsScalaMap(this.externalConf.getSparkConf))
      }
    }

    tmpConf
  }

  /**
   * 在加载任务配置文件前将被加载
   */
  override private[fire] def loadConf: Unit = {
    PropUtils.load("spark-streaming.properties")
  }

  /**
   * Streaming的处理过程强烈建议放到process中，保持风格统一
   * 注：此方法会被自动调用，在以下两种情况下，必须将逻辑写在process中
   * 1. 开启checkpoint
   * 2. 支持streaming热重启（可在不关闭streaming任务的前提下修改batch时间）
   */
  override def process: Unit = {
    ValueUtils.requireNull(this.checkPointDir, "当开启checkPoint机制时，必须将对接kafka的代码写在process方法内")
    ValueUtils.requireNull(this.externalConf, "当需要使用热重启功能时，必须将对接kafka的代码写在process方法内")
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
  @Deprecated
  def kafkaParams(groupId: String = this.appName, kafkaBrokers: String = null, offset: String = FireKafkaConf.offsetLargest, autoCommit: Boolean = false, keyNum: Int = 1): Map[String, Object] = {
    KafkaUtils.kafkaParams(null, groupId, kafkaBrokers, offset, autoCommit, keyNum)
  }

  /**
   * 用于重置StreamingContext（仅支持batch时间的修改）
   *
   * @return
   * 响应结果
   */
  @Rest("/system/streaming/hotRestart")
  def hotRestart(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      this.externalConf = JSON.parseObject(json, classOf[RestartParams])
      new Thread(new Runnable {
        override def run(): Unit = {
          ssc.stop(externalConf.isRestartSparkContext, externalConf.isStopGracefully)
          init(externalConf.getBatchDuration, externalConf.isCheckPoint)
        }
      }).start()

      this.logFire(s"[hotRestart] 执行热重启成功：duration=${this.externalConf.getBatchDuration} json=$json", "rest")
      msg.buildSuccess(s"执行热重启成功：duration=${this.externalConf.getBatchDuration}", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[hotRestart] 执行热重启成功失败：json=$json", "rest", throwable = e)
        msg.buildError("执行热重启成功失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

}
