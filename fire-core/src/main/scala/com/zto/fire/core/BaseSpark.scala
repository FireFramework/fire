package com.zto.fire.core

import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, TimeUnit}

import com.zto.fire.common.acc.{AccumulatorManager, LogAccumulator}
import com.zto.fire.common.db.JdbcOper
import com.zto.fire.common.enu.JobType
import com.zto.fire.common.util._
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.ext.module.{HBaseContextExt, KuduContextExt}
import com.zto.fire.core.rest.{RestfulRegister, SystemRestful}
import com.zto.fire.core.util.{SingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import spark.Spark

/**
  * Spark通用父类
  * Created by ChengLong on 2018-03-06.
  */
trait BaseSpark extends SparkListener with Logging with Serializable {
  var conf: SparkConf = _
  var spark: SparkSession = _
  var sc: SparkContext = _
  var catalog: Catalog = _
  val jdbc = JdbcOper
  var ssc: StreamingContext = _
  var hiveContext: SQLContext = _
  var sqlContext: SQLContext = _
  var kuduContext: KuduContextExt = _
  var hbaseContext: HBaseContextExt = _
  val startTime = DateFormatUtils.currentTime
  val driverClass = this.getClass.getSimpleName.replace("$", "")
  var appName = this.driverClass
  val className = this.getClass.getName.replace("$", "")
  val jobType = JobType.UNDEFINED
  lazy val threadPool = Executors.newFixedThreadPool(20)
  lazy val threadPoolSchedule = Executors.newScheduledThreadPool(10)
  val restPort = SystemInfoUtils.getRundomPort
  private[fire] var restfulRegister: RestfulRegister = _
  private[fire] var systemRestful: SystemRestful = _
  var args: Array[String] = _
  val count: LongAccumulator = new LongAccumulator
  val logAccumulator = new LogAccumulator
  var applicationId: String = _
  var batchDuration: Long = _
  var webUI: String = _
  this.boot

  /**
    * 生命周期方法：初始化fire框架必要的信息
    * 注：该方法会同时在driver端与executor端执行
    */
  private[this] final def boot: Unit = {
    PropUtils.load(this.appName)
    PropUtils.setProperty("spark.driver.class.name", this.className)
    if (StringUtils.isNotBlank(GlobalConstants.SparkConf.appName)) {
      this.appName = GlobalConstants.SparkConf.appName
    }
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)
    this.wrapLogWarn("完成fire框架启动...")
  }

  /**
    * 生命周期方法：用于在SparkSession初始化之前完成用户需要的动作
    * 注：该方法会在进行init之前自动被系统调用
    * @param args
    * main方法参数
    */
  def before(args: Array[String]): Unit = {}

  /**
    * 生命周期方法：初始化spark运行信息
    *
    * @param conf
    *             Spark配置信息
    * @param args main方法参数
    */
  def init(conf: SparkConf = null, args: Array[String] = null): Unit = {
    this.before(args)
    this.wrapLogWarn("完成用户资源初始化")
    this.args = args
    this.createContext(conf)
  }

  /**
    * 生命周期方法：具体的用户开发的业务逻辑代码
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  def process: Unit

  /**
    * 生命周期方法：用于关闭SparkContext
    */
  final def stop: Unit = {
    if (this.spark != null && this.sc != null && !this.sc.isStopped) {
      this.spark.stop()
    }
  }

  /**
    * 生命周期方法：用于资源回收与清理，子类复写实现具体逻辑
    * 注：该方法会在进行destroy之前自动被系统调用
    */
  def after(args: Array[String] = this.args): Unit = {}

  /**
    * 生命周期方法：进行fire框架的资源回收
    * 注：不允许子类覆盖
    */
  private[fire] final def shutdown(stopGracefully: Boolean = true): Unit = {
    try {
      this.wrapLogWarn("完成用户资源回收...")

      if (stopGracefully) {
        if (this.sqlContext != null) this.sqlContext.clearCache
        if (this.ssc != null) {
          this.ssc.stop(true, stopGracefully)
          this.ssc = null
          this.sc = null
        }
        if (this.sc != null && !this.sc.isStopped) {
          this.sc.stop()
          this.sc = null
        }
      }

      if (this.threadPool != null && !this.threadPool.isShutdown) {
        this.threadPool.shutdownNow()
      }
      if (this.threadPoolSchedule != null && !this.threadPoolSchedule.isShutdown) {
        this.threadPoolSchedule.shutdownNow()
      }
      Spark.stop()
      this.wrapLogWarn("完成fire资源回收...")
    } finally {
      GlobalConstants.PrintModule.END_TIME_COST(this.startTime)
    }
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
  private[this] final def createContext(conf: SparkConf): Unit = {
    this.restfulRegister = new RestfulRegister(this.threadPool).port(restPort)
    this.systemRestful = new SystemRestful(this)

    // 注册到zrc平台，并覆盖配置信息
    if (this.jobType != JobType.CORE) PropUtils.invokeZrcConf(this.className, s"${SystemInfoUtils.getIp}:${this.restPort}")
    PropUtils.print()
    val tmpConf = if (conf == null) this.buildConf(conf) else conf
    tmpConf.setAll(PropUtils.toMap)
    tmpConf.set("spark.driver.class.simple.name", this.driverClass)
    if (SystemInfoUtils.isWindows) {
      this.spark = SparkSession.builder().config(tmpConf).master("local[*]") /*.enableHiveSupport()*/ .getOrCreate()
    } else {
      this.spark = SparkSession.builder().config(tmpConf).enableHiveSupport().getOrCreate()
    }
    SingletonFactory.setSparkSession(this.spark)
    this.spark.registerAll()
    this.sc = this.spark.sparkContext
    this.catalog = this.spark.catalog
    this.sc.setLogLevel(GlobalConstants.SparkConf.logLevel)
    this.sc.addSparkListener(new BaseSparkListener(this))
    this.initLogging(this.className)
    AccumulatorManager.registerAccumulators(this.sc, this.accumulatorMap)
    this.hiveContext = this.spark.sqlContext
    this.sqlContext = this.hiveContext
    this.hbaseContext = SingletonFactory.getHBaseContextInstance(this.sc)
    this.kuduContext = SingletonFactory.getKuduContextInstance(this.sc)
    this.applicationId = SparkUtils.getApplicationId(this.spark)
    this.webUI = SparkUtils.getWebUI(this.spark)
    this.conf = tmpConf
  }

  /**
    * 内置累加器列表
    */
  private[fire] def accumulatorMap: Map[String, AccumulatorV2[_, _]] = {
    Map(AccumulatorManager.log -> this.logAccumulator, AccumulatorManager.counter -> this.count)
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
  def runAsThread(fun: => Unit, threadCount: Int = 1, debug: Boolean = false, threadPool: ExecutorService = this.threadPool): Unit = {
    ThreadUtils.runAsThread(threadPool, fun, threadCount, debug)
  }

  /**
    * 以子线程while循环方式循环执行函数调用
    *
    * @param fun
    * 用于指定以多线程方式执行的函数
    * @param delay
    * 循环调用间隔时间（单位s）
    */
  def runAsThreadLoop(fun: => Unit, delay: Long = 10, threadCount: Int = 1, debug: Boolean = false, threadPool: ExecutorService = this.threadPool): Unit = {
    ThreadUtils.runAsThreadLoop(threadPool, fun, delay, threadCount, debug)
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
  def runAsSchedule(fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1, debug: Boolean = false, threadPoolSchedule: ScheduledExecutorService = this.threadPoolSchedule): Unit = {
    ThreadUtils.runAsSchedule(threadPoolSchedule, fun, initialDelay, period, rate, timeUnit, threadCount, debug)
  }
}