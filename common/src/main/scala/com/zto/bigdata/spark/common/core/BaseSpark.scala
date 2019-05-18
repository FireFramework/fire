package com.zto.bigdata.spark.common.core

import java.util.concurrent.{Executors, TimeUnit}

import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.ext.{HBaseContextExt, KuduContextExt}
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
  val className = this.getClass.getName.replace("$", "")
  lazy val threadPool = Executors.newFixedThreadPool(20)
  lazy val threadPoolSchedule = Executors.newScheduledThreadPool(10)
  val restPort = SystemInfoUtils.getRundomPort
  val restfulRegister = new RestfulRegister(this.threadPool).port(restPort)
  private val systemRestful = new SystemRestful(this)
  val log = LoggerFactory.getLogger(this.getClass)
  val logger = new LogUtils(log)
  var applicationId: String = _
  var batchDuration: Long = _
  var webUI: String = _
  this.init

  /**
    * 初始化，系统启动时默认执行
    */
  private[this] def init: Unit = {
    PropUtils.load(this.appName)
    // 后续调用接口获取配置信息即可
    /*val map = JSON.parseObject("{id: '10', name:'root'}", classOf[java.util.Map[String, String]]).toScalaMap
    PropUtils.setProperties(map)*/
    if (StringUtils.isNotBlank(GlobalConstants.SparkConf.appName)) {
      this.appName = GlobalConstants.SparkConf.appName
    }
    PropUtils.print()
  }

  /**
    * 程序初始化方法，用于初始化必要的值
    *
    * @param conf
    *             Spark配置信息
    * @param args main方法参数
    */
  def init(conf: SparkConf = null, args: Array[String] = null): Unit = {
    this.createContext(conf)
  }

  /**
    * 构建或合并SparkConf
    * 注：不同的子类需根据需要复写该方法
    *
    * @param conf
    * 在conf基础上构建
    * @return
    * 合并后的SparkConf对象
    */
  def buildConf(conf: SparkConf = null): SparkConf

  /**
    * 构建一系列context对象
    */
  def createContext(conf: SparkConf): Unit = {
    val tmpConf = if (conf == null) this.buildConf(conf) else conf
    tmpConf.setAll(PropUtils.toMap)
    if (SystemInfoUtils.isWindows) {
      this.spark = SparkSession.builder().config(tmpConf).master("local[*]") /*.enableHiveSupport()*/ .getOrCreate()
    } else {
      this.spark = SparkSession.builder().config(tmpConf).enableHiveSupport().getOrCreate()
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
    this.conf = tmpConf
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  def process: Unit

  /**
    * 资源回收与清理，子类复写实现具体逻辑
    * 注：该方法会在进行destroy之前自动被系统调用
    *
    * @param args
    */
  def release(args: Array[String] = null): Unit = {}

  /**
    * 打印总耗时
    *
    * @param applicationEnd
    * 整个job执行结束后执行
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (this.hiveContext != null) this.hiveContext.clearCache
    this.release()
    this.threadPool.shutdownNow()
    this.threadPoolSchedule.shutdownNow()
    logger.wrapLogWarn("完成用户资源回收")
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
    * 资源回收与应用关闭
    * 注：不允许子类覆盖
    */
  final def destroy: Unit = {
    if (this.ssc == null) {
      this.spark.stop()
    } else {
      this.ssc.stop(true, false)
    }
    this.threadPool.shutdownNow()
    this.threadPoolSchedule.shutdownNow()
    Spark.stop()
    logger.wrapLogWarn("完成spark资源回收")
  }

}