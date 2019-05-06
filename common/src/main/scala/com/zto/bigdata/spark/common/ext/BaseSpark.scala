package com.zto.bigdata.spark.common.ext

import java.util.concurrent.{Executors, TimeUnit}

import com.zto.bigdata.spark.common.rest.{RestfulRegister, SystemRestful}
import com.zto.bigdata.spark.common.util._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import spark.Spark

/**
  * Spark通用父类
  * Created by ChengLong on 2018-03-06.
  */
trait BaseSpark extends SparkListener with Serializable {
  var conf: SparkConf = _
  var spark: SparkSession = _
  var sc: SparkContext = _
  var ssc: StreamingContext = _
  var hiveContext: SQLContext = _
  var sqlContext: SQLContext = _
  var kuduContext: KuduContextExt = _
  var hbaseContext: HBaseContextExt = _
  val startTime = SparkUtils.currentTime
  var appName = this.getClass.getSimpleName.replace("$", "")
  lazy val threadPool = Executors.newFixedThreadPool(20)
  lazy val threadPoolSchedule = Executors.newScheduledThreadPool(10)
  val restPort = SystemInfoUtils.getRundomPort
  val restfulRegister = new RestfulRegister(this.threadPool).port(restPort)
  private val systemRestful = new SystemRestful(this)
  val log = LoggerFactory.getLogger(this.getClass)
  val logger = new LogUtils(log)
  var applicationId: String = _
  var webUI: String = _
  this.init

  /**
    * 初始化
    */
  private[this] def init: Unit = {
    PropUtils.load(this.appName)
    if (StringUtils.isNotBlank(GlobalConstants.SparkConf.appName)) {
      this.appName = GlobalConstants.SparkConf.appName
    }
    PropUtils.print()
  }

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param appName
    * job名称
    * @param conf
    * Spark配置信息
    */
  def init(appName: String = this.appName, conf: SparkConf = null): Unit

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  def process: Unit

  /**
    * 打印总耗时
    *
    * @param applicationEnd
    * 整个job执行结束后执行
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (this.hiveContext != null) this.hiveContext.clearCache
    GlobalConstants.PrintModule.END_TIME_COST(this.startTime)
  }

  /**
    * 以子线程方式执行函数调用
    *
    * @param fun
    * 用于指定以多线程方式执行的函数
    * @param threadCount
    * 表示开启多少个线程执行该fun任务
    * @param debug
    * true：打印运行日志
    * false：不打印运行日志
    */
  def runAsThread(fun: => Unit, threadCount: Int = 1, debug: Boolean = false): Unit = {
    ThreadUtils.runAsThread(this.threadPool, fun, threadCount, debug)
  }

  /**
    * 以子线程while循环方式循环执行函数调用
    *
    * @param fun
    * 用于指定以多线程方式执行的函数
    * @param delay
    * 循环调用间隔时间（单位s）
    */
  def runAsThreadLoop(fun: => Unit, delay: Long = 10, threadCount: Int = 1, debug: Boolean = false): Unit = {
    ThreadUtils.runAsThreadLoop(this.threadPool, fun, delay, threadCount, debug)
  }

  /**
    * 定时调度给定的函数
    *
    * @param fun
    * 定时执行的任务函数引用
    * @param initialDelay
    * 第一次延迟执行的时长
    * @param period
    * 每隔指定的时长执行一次
    * @param rate
    * true：表示周期性的执行，不受上一个定时任务的约束
    * false：表示当上一次周期性任务执行成功后，period后开始执行
    * @param timeUnit
    * 时间单位，默认分钟
    * @param threadCount
    * 表示开启多少个线程执行该fun任务
    * @param debug
    * true：打印运行日志
    * false：不打印运行日志
    */
  def runAsSchedule(fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1, debug: Boolean = false): Unit = {
    ThreadUtils.runAsSchedule(this.threadPoolSchedule, fun, initialDelay, period, rate, timeUnit, threadCount, debug)
  }

  /**
    * 根据key获取配置信息
    *
    * @param key
    * properties中的key
    * @return
    * 配置的值
    */
  def getConf(key: String): String = {
    PropUtils.getString(key)
  }

  /**
    * 获取appName
    * @return
    */
  def getAppName: String = {
    this.appName
  }

  /**
    * 销毁
    */
  def destory: Unit = {
    if (this.ssc == null) {
      this.spark.stop()
    } else {
      this.ssc.stop(true, false)
    }
    this.threadPool.shutdownNow()
    this.threadPoolSchedule.shutdownNow()
    Spark.stop()
  }
}