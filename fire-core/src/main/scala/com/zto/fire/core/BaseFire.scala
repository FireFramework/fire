package com.zto.fire.core

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ExecutorService, ScheduledExecutorService, TimeUnit}

import com.zto.fire.common.db.JdbcOper
import com.zto.fire.common.enu.{JobType, ThreadPoolType}
import com.zto.fire.common.task.SchedulerManager
import com.zto.fire.common.util.{DateFormatUtils, GlobalConstants, PropUtils, SystemInfoUtils, ThreadUtils, ValueUtils}
import com.zto.fire.core.rest.{RestfulRegister, SparkSystemRestful, SystemRestful}
import spark.Spark

/**
 * 通用的父接口，提供通用的生命周期方法约束
 *
 * @author ChengLong 2020年1月7日 09:20:02
 * @since 0.4.1
 */
trait BaseFire {
  // 任务启动时间戳
  val startTime = DateFormatUtils.currentTime
  // jdbc包装类
  val jdbc = JdbcOper
  // web ui地址
  var webUI: String = _
  val value = ValueUtils
  // main方法参数
  var args: Array[String] = _
  // yarn任务的applicationId
  var applicationId: String = _
  // 当前任务的类型标识
  val jobType = JobType.UNDEFINED
  // fire框架内置的restful接口
  private[fire] var systemRestful: SystemRestful = _
  // restful接口注册
  private[fire] var restfulRegister: RestfulRegister = _
  // fire restful服务端口号
  val restPort = SystemInfoUtils.getRundomPort
  // 用于子类的锁状态判断，默认关闭状态
  lazy val lock = new AtomicBoolean(false)
  // 是否已停止
  lazy val isStoped = new AtomicBoolean(false)
  // 当前任务的类名（包名+类名）
  val className = this.getClass.getName.replace("$", "")
  // 当前任务的类名
  val driverClass = this.getClass.getSimpleName.replace("$", "")
  // 默认的任务名称为类名
  var appName = this.driverClass
  // fire内置线程池
  lazy val threadPool = ThreadUtils.createThreadPool("threadPool", ThreadPoolType.FIXED, 10)
  lazy val threadPoolSchedule = ThreadUtils.createThreadPool("threadPoolSchedule", ThreadPoolType.SCHEDULED, 10).asInstanceOf[ScheduledExecutorService]
  this.boot

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  private[fire] def boot: Unit

  /**
   * 生命周期方法：用于在SparkSession初始化之前完成用户需要的动作
   * 注：该方法会在进行init之前自动被系统调用
   *
   * @param args
   * main方法参数
   */
  def before(args: Array[String]): Unit = {}

  /**
   * 生命周期方法：初始化运行信息
   *
   * @param conf 配置信息
   * @param args main方法参数
   */
  def init(conf: Any = null, args: Array[String] = null): Unit = {
    this.before(args)
    println(s" ${GlobalConstants.PS1.YELLOW}< ----------------------------------- 完成用户资源初始化，任务类型：${this.jobType.getJobType} ---------------------------------- > ${GlobalConstants.PS1.DEFAULT}")
    this.args = args
    this.createContext(conf)
  }

  /**
   * 创建计算引擎运行时环境
   *
   * @param conf
   * 配置信息
   */
  private[fire] def createContext(conf: Any): Unit

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  def process: Unit

  /**
   * 生命周期方法：用于资源回收与清理，子类复写实现具体逻辑
   * 注：该方法会在进行destroy之前自动被系统调用
   */
  def after(args: Array[String] = null): Unit = {}

  /**
   * 生命周期方法：用于回收资源
   */
  def stop: Unit

  /**
   * 生命周期方法：进行fire框架的资源回收
   * 注：不允许子类覆盖
   */
  private[fire] def shutdown(stopGracefully: Boolean = true): Unit = {
    if (this.isStoped.compareAndSet(false, true)) {
      ThreadUtils.shutdown
      Spark.stop()
      SchedulerManager.shutdown(stopGracefully)
      println("---> 完成fire资源回收 <---")
      GlobalConstants.PrintModule.END_TIME_COST(this.startTime)
      // TODO: yarn kill; system.exit(0)
    }
  }

  /**
   * 以子线程方式执行函数调用
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   * @param threadCount
   * 表示开启多少个线程执行该fun任务
   */
  def runAsThread(fun: => Unit, threadCount: Int = 1, threadPool: ExecutorService = this.threadPool): Unit = {
    ThreadUtils.runAsThread(threadPool, fun, threadCount)
  }

  /**
   * 以子线程while循环方式循环执行函数调用
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   * @param delay
   * 循环调用间隔时间（单位s）
   */
  def runAsThreadLoop(fun: => Unit, delay: Long = 10, threadCount: Int = 1, threadPool: ExecutorService = this.threadPool): Unit = {
    ThreadUtils.runAsThreadLoop(threadPool, fun, delay, threadCount)
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
   */
  def runAsSchedule(fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1, threadPoolSchedule: ScheduledExecutorService = this.threadPoolSchedule): Unit = {
    ThreadUtils.runAsSchedule(threadPoolSchedule, fun, initialDelay, period, rate, timeUnit, threadCount)
  }

  /**
   * 用于在fire框架启动时展示信息
   */
  private[fire] def splash: Unit = {
    val info =
      """
        |       ___                       ___           ___
        |     /\  \          ___        /\  \         /\  \
        |    /::\  \        /\  \      /::\  \       /::\  \
        |   /:/\:\  \       \:\  \    /:/\:\  \     /:/\:\  \
        |  /::\~\:\  \      /::\__\  /::\~\:\  \   /::\~\:\  \
        | /:/\:\ \:\__\  __/:/\/__/ /:/\:\ \:\__\ /:/\:\ \:\__\
        | \/__\:\ \/__/ /\/:/  /    \/_|::\/:/  / \:\~\:\ \/__/
        |      \:\__\   \::/__/        |:|::/  /   \:\ \:\__\
        |       \/__/    \:\__\        |:|\/__/     \:\ \/__/
        |                 \/__/        |:|  |        \:\__\
        |                               \|__|         \/__/     version
        |
        |""".stripMargin.replace("version", s"version ${GlobalConstants.PS1.PINK + PropUtils.getString("spark.fire.version", "1.0.0")}")

    println(GlobalConstants.PS1.GREEN + info + GlobalConstants.PS1.DEFAULT)
  }
}
