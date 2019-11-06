package com.zto.fire.common.acc

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import com.google.common.collect.HashBasedTable
import com.zto.fire.common.bean.TimeCost
import com.zto.fire.common.task.SchedulerManager
import com.zto.fire.common.util.{GlobalConstants, StringsUtils, SystemInfoUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkContext, SparkEnv}

import scala.collection.mutable

/**
 * fire内置累加器工具类
 *
 * @author ChengLong 2019-7-25 19:11:16
 */
private[fire] object AccumulatorManager {
  // 累加器名称，含有fire的名字将会显示在webui中
  private[this] val counterLabel = "fire-counter"
  private[fire] val counter = new LongAccumulator

  // 日志累加器
  private[this] val logAccumulatorLabel = "logAccumulator"
  private[fire] val logAccumulator = new LogAccumulator

  // 多值累加器
  private[this] val multiCounterLabel = "fire-multiCounter"
  private[fire] val multiCounter = new MultiCounterAccumulator

  // timer累加器
  private[this] val multiTimerLabel = "multiTimer"
  private[fire] val multiTimer = new MultiTimerAccumulator

  private[this] val accMap = Map(this.logAccumulatorLabel -> this.logAccumulator, this.counterLabel -> this.counter, this.multiCounterLabel -> this.multiCounter, this.multiTimerLabel -> this.multiTimer)
  private[this] val initExecutors: AtomicInteger = new AtomicInteger(0)

  // 获取当前任务的全类名
  private[this] lazy val jobClassName = SparkEnv.get.conf.get("spark.driver.class.name", "")
  // 用于注册定时任务的列表
  private[this] val taskRegisterSet = mutable.HashSet[Object]()

  /**
   * 注册定时任务实例
   */
  def registerTasks(tasks: Object *): Unit = {
    if (tasks != null) {
      tasks.foreach(taskInstances => taskRegisterSet.add(taskInstances))
    }
  }

  /**
   * 将数据累加到count累加器中
   *
   * @param value
   * 累加值
   */
  def addCounter(value: Long): Unit = {
    if (SparkEnv.get != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
      val countAccumulator = SparkEnv.get.conf.get(this.counterLabel, "")
      if (StringUtils.isNotBlank(countAccumulator)) {
        val counter: LongAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(countAccumulator)))
        counter.add(value)
      }
    } else {
      this.counter.add(value)
    }
  }

  /**
   * 获取counter累加器的值
   *
   * @return
   * 累加结果
   */
  def getCounter: Long = this.counter.value

  /**
   * 将timeCost累加到日志累加器中
   *
   * @param timeCost
   * TimeCost实例对象
   */
  def addLog(timeCost: TimeCost): Unit = {
    val env = SparkEnv.get
    if (env != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
      val logAccumulator = SparkEnv.get.conf.get(this.logAccumulatorLabel, "")
      if (StringUtils.isNotBlank(logAccumulator)) {
        val logAcc: LogAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(logAccumulator)))
        logAcc.add(timeCost)
      }
    } else {
      this.logAccumulator.add(timeCost)
    }
  }

  /**
   * 获取日志累加器中的值
   *
   * @return
   * 日志累加值
   */
  def getLog: ConcurrentLinkedQueue[String] = this.logAccumulator.value

  /**
   * 将数据累加到multiCount累加器中
   *
   * @param value
   * 累加值
   */
  def addMultiCounter(key: String, value: Long): Unit = {
    if (SparkEnv.get != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
      val countAccumulator = SparkEnv.get.conf.get(this.multiCounterLabel, "")
      if (StringUtils.isNotBlank(countAccumulator)) {
        val multiCounter: MultiCounterAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(countAccumulator)))
        multiCounter.add(key, value)
      }
    } else {
      this.multiCounter.add(key, value)
    }
  }

  /**
   * 获取multiCounter累加器的值
   *
   * @return
   * 累加结果
   */
  def getMultiCounter: ConcurrentHashMap[String, Long] = this.multiCounter.value

  /**
   * 将数据累加到timer累加器中
   *
   * @param value
   * 累加值的key、value和时间的schema，默认为yyyy-MM-dd HH:mm:00
   */
  def addMultiTimer(key: String, value: Long, schema: String = GlobalConstants.MultiTimerSchema.MIN): Unit = {
    if (SparkEnv.get != null && !"driver".equalsIgnoreCase(SparkEnv.get.executorId)) {
      val timerAccumulator = SparkEnv.get.conf.get(this.multiTimerLabel, "")
      if (StringUtils.isNotBlank(timerAccumulator)) {
        val multiTimer: MultiTimerAccumulator = SparkEnv.get.closureSerializer.newInstance.deserialize(ByteBuffer.wrap(StringsUtils.toByteArray(timerAccumulator)))
        multiTimer.add(key, value, schema)
      }
    } else {
      this.multiTimer.add(key, value, schema)
    }
  }

  /**
   * 用于构建复杂类型（json）的多时间维度累加器的key
   * 并将key作为多时间维度累加器的key
   *
   * @param value
   * 累加的值
   * @param cluster
   * 连接的集群名
   * @param module
   * 所在的模块
   * @param method
   * 所在的方法名
   * @param action
   * 执行的动作
   * @param sink
   * 作用的目标
   * @param level
   * 日志级别：INFO、ERROR
   * @return
   * 累加器的key（json格式）
   */
  def addMultiTimer(module: String, method: String, action: String, sink: String, level: String, cluster: String, value: Long): Unit = {
    val multiKey = s"""{"cluster":"$cluster","module":"$module","method":"$method","action":"$action","sink":"$sink","level":"$level","jobClass":"$jobClassName"}"""
    this.addMultiTimer(multiKey, value)
  }

  /**
   * 获取timer累加器的值
   *
   * @return
   * 累加结果
   */
  def getMultiTimer: HashBasedTable[String, String, Long] = this.multiTimer.value

  /**
   * 注册多个自定义累加器到每个executor
   *
   * @param sc
   * SparkContext
   * [key, accumulator]
   */
  private[fire] def registerAccumulators(sc: SparkContext): Unit = {
    if (sc != null && accMap != null && accMap.size > 0) {
      if (this.initExecutors.get() == 0) this.initExecutors.set(sc.getConf.get("spark.executor.instances", if (SystemInfoUtils.isLinux) "10000" else "10").toInt)
      // 将定时任务所在类的实例广播到每个executor端
      val taskSet = sc.broadcast(taskRegisterSet)

      // 序列化内置的累加器
      val accumulatorMap = accMap.map(accInfo => {
        // 注册每个累加器，必须是合法的名称并且未被注册过
        if (accInfo._2 != null && !accInfo._2.isRegistered) {
          if (StringUtils.isNotBlank(accInfo._1) && accInfo._1.contains("fire")) {
            sc.register(accInfo._2, accInfo._1)
          } else {
            sc.register(accInfo._2)
          }
        }
        (accInfo._1, SparkEnv.get.closureSerializer.newInstance().serialize(accInfo._2).array())
      })

      // 获取申请的executor数，设置累加器到conf中
      val rdd = sc.parallelize(1 to this.initExecutors.get, this.initExecutors.get)
      rdd.foreachPartition(i => {
        // 将序列化后的累加器放置到conf中
        accumulatorMap.foreach(accSer => SparkEnv.get.conf.set(accSer._1, StringsUtils.toHexString(accSer._2)))
        if (GlobalConstants.scheduleEnable) {
          // 从广播中获取到定时任务的实例，并在executor端完成注册
          val tasks = taskSet.value
          if (tasks != null && tasks.size > 0) {
            tasks.foreach(obj => {
              // 防止生成重复的Scheduler实例
              if (!SchedulerManager.schedulerIsStarted()) {
                SchedulerManager.registerTasks(obj)
              }
            })
          }
        }
      })
    }
  }
}
