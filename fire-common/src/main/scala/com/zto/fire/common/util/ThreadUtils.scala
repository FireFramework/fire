package com.zto.fire.common.util

import java.util.Objects
import java.util.concurrent.{ExecutorService, ScheduledExecutorService, TimeUnit}

import com.zto.fire.common.bean.BaseLogging

/**
  * 线程相关工具类
  *
  * @author ChengLong 2019-4-25 15:17:55
  */
object ThreadUtils extends BaseLogging {

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
}
