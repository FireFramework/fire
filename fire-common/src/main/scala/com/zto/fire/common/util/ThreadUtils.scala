package com.zto.fire.common.util

import java.util.Objects
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, ScheduledExecutorService, TimeUnit}

import com.zto.fire.common.bean.BaseLogging
import com.zto.fire.common.enu.ThreadPoolType
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions

/**
 * 线程相关工具类
 *
 * @author ChengLong 2019-4-25 15:17:55
 */
object ThreadUtils extends BaseLogging {
  // 用于维护使用ThreadUtils创建的线程池对象，并进行统一的关闭
  private val threadPoolMap = new ConcurrentHashMap[String, ExecutorService]()

  /**
   * 以子线程方式执行函数调用
   *
   * @param threadPool
   * 线程池
   * @param fun
   * 用于指定以多线程方式执行的函数
   * @param threadCount
   * 表示开启多少个线程执行该fun任务
   */
  def runAsThread(threadPool: ExecutorService, fun: => Unit, threadCount: Int = 1): Unit = {
    Objects.requireNonNull(threadPool, "线程池不能为空")

    (1 to threadCount).foreach(_ => {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          mark()
          fun
          log(s"Invoke runAsThread as ${Thread.currentThread().getName}.")
        }
      })
    })
  }

  /**
   * 以子线程while方式循环执行函数调用
   *
   * @param fun
   * 用于指定以多线程方式执行的函数
   * @param delay
   * 循环调用间隔时间（单位s）
   */
  def runAsThreadLoop(threadPool: ExecutorService, fun: => Unit, delay: Long = 10, threadCount: Int = 1): Unit = {
    Objects.requireNonNull(threadPool, "线程池不能为空")

    (1 to threadCount).foreach(_ => {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          while (true) {
            mark
            fun
            log(s"Loop invoke runAsThreadLoop as ${Thread.currentThread().getName}. Delay is ${delay}s.")
            Thread.sleep(delay * 1000)
          }
        }
      })
    })
  }

  /**
   * 定时调度给定的函数
   *
   * @param threadPoolSchedule
   * 定时调度线程池
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
  def runAsSchedule(threadPoolSchedule: ScheduledExecutorService, fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1): Unit = {
    Objects.requireNonNull(threadPoolSchedule, "线程池不能为空")

    (1 to threadCount).foreach(_ => {
      if (rate) {
        // 表示周期性的执行，不受上一个定时任务的约束
        threadPoolSchedule.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = {
            wrapFun()
          }
        }, initialDelay, period, timeUnit)
      } else {
        // 表示当上一次周期性任务执行成功后，period后开始执行
        threadPoolSchedule.scheduleWithFixedDelay(new Runnable {
          override def run(): Unit = {
            wrapFun()
          }
        }, initialDelay, period, timeUnit)
      }

      // 处理传入的函数
      def wrapFun(): Unit = {
        mark()
        fun
        log(s"Loop invoke runAsSchedule as ${Thread.currentThread().getName}. Delay is ${period}${timeUnit.name()}.")
      }
    })
  }

  /**
   * 表示当上一次周期性任务执行成功后
   * period后开始执行给定的函数fun
   *
   * @param threadPoolSchedule
   * 定时调度线程池
   * @param fun
   * 定时执行的任务函数引用
   * @param initialDelay
   * 第一次延迟执行的时长
   * @param period
   * 每隔指定的时长执行一次
   * @param timeUnit
   * 时间单位，默认分钟
   * @param threadCount
   * 表示开启多少个线程执行该fun任务
   */
  def runAsScheduleAtFixedRate(threadPoolSchedule: ScheduledExecutorService, fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1): Unit = {
    this.runAsSchedule(threadPoolSchedule, fun, initialDelay, period, true, timeUnit, threadCount)
  }

  /**
   * 表示当上一次周期性任务执行成功后，period后开始执行fun函数
   * 注：受上一个定时任务的影响
   *
   * @param threadPoolSchedule
   * 定时调度线程池
   * @param fun
   * 定时执行的任务函数引用
   * @param initialDelay
   * 第一次延迟执行的时长
   * @param period
   * 每隔指定的时长执行一次
   * @param timeUnit
   * 时间单位，默认分钟
   * @param threadCount
   * 表示开启多少个线程执行该fun任务
   */
  def runAsScheduleWithFixedDelay(threadPoolSchedule: ScheduledExecutorService, fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1): Unit = {
    this.runAsSchedule(threadPoolSchedule, fun, initialDelay, period, false, timeUnit, threadCount)
  }

  /**
   * 创建一个新的指定大小的调度线程池
   * 如果名称已存在，则直接返回对应的线程池
   *
   * @param poolName
   * 线程池标识
   * @param poolType
   * 线程池类型
   * @param poolSize
   * 线程池大小
   */
  def createThreadPool(poolName: String, poolType: ThreadPoolType = ThreadPoolType.FIXED, poolSize: Int = 1): ExecutorService = {
    ParamUtils.requireNonNullForce(poolName, "线程池名称不能为空")
    if (this.threadPoolMap.containsKey(poolName)) {
      this.threadPoolMap.get(poolName)
    } else {
      val threadPool = poolType match {
        case ThreadPoolType.FIXED => Executors.newFixedThreadPool(poolSize)
        case ThreadPoolType.SCHEDULED => Executors.newScheduledThreadPool(poolSize)
        case ThreadPoolType.SINGLE => Executors.newSingleThreadExecutor()
        case ThreadPoolType.CACHED => Executors.newCachedThreadPool()
        case ThreadPoolType.WORK_STEALING => Executors.newWorkStealingPool()
        case _ => Executors.newFixedThreadPool(poolSize)
      }
      this.threadPoolMap.put(poolName, threadPool)
      threadPool
    }
  }

  /**
   * 用于释放指定的线程池
   *
   * @param poolName
   * 线程池标识
   */
  def shutdown(poolName: String): Unit = {
    if (StringUtils.isNotBlank(poolName) && this.threadPoolMap.containsKey(poolName)) {
      val threadPool = this.threadPoolMap.get(poolName)
      if (threadPool != null && !threadPool.isShutdown) threadPool.shutdownNow()
    }
  }

  /**
   * 用于释放所有线程池
   */
  private[fire] def shutdown: Unit = {
    val poolNum = this.threadPoolMap.size()
    if (this.threadPoolMap.size() > 0) {
      JavaConversions.mapAsScalaConcurrentMap(this.threadPoolMap).foreach(pool => {
        if (pool != null && pool._2 != null) {
          pool._2.shutdownNow()
          println(s"${GlobalConstants.PS1.GREEN}---> 完成线程池[ ${pool._1} ]的资源回收. <---${GlobalConstants.PS1.DEFAULT}")
        }
      })
    }
    println(s"${GlobalConstants.PS1.PINK}---> 完成所有线程池回收，总计：${poolNum}个. <---${GlobalConstants.PS1.DEFAULT}")
  }
}
