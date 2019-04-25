package com.zto.bigdata.spark.common.util

import java.util.Objects
import java.util.concurrent.{ExecutorService, ScheduledExecutorService, TimeUnit}

/**
  * 线程相关工具类
  *
  * @author ChengLong 2019-4-25 15:17:55
  */
object ThreadUtils {

  /**
    * 以子线程方式执行函数调用
    *
    * @param threadPool
    * 线程池
    * @param fun
    * 用于指定以多线程方式执行的函数
    * @param threadCount
    * 表示开启多少个线程执行该fun任务
    * @param debug
    * true：打印运行日志
    * false：不打印运行日志
    */
  def runAsThread(threadPool: ExecutorService, fun: => Unit, threadCount: Int = 1, debug: Boolean = false): Unit = {
    Objects.requireNonNull(threadPool, "线程池不能为空")

    (1 to threadCount).foreach(_ => {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          val startTime = System.currentTimeMillis()
          fun
          if (debug) println(s"--> ${GlobalConstants.PS1.GREEN}Invoke ${fun.getClass.getName} as ${Thread.currentThread().getName}. Time: ${DateFormatUtils.formatCurrentDateTime()}. TimeCost: ${System.currentTimeMillis() - startTime}. ${GlobalConstants.PS1.DEFAULT}<--")
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
  def runAsThreadLoop(threadPool: ExecutorService, fun: => Unit, delay: Long = 10, threadCount: Int = 1, debug: Boolean = false): Unit = {
    Objects.requireNonNull(threadPool, "线程池不能为空")

    (1 to threadCount).foreach(_ => {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          while (true) {
            val startTime = System.currentTimeMillis()
            fun
            if (debug) println(s"--> ${GlobalConstants.PS1.GREEN}Loop invoke ${fun.getClass.getName} as ${Thread.currentThread().getName}. Delay is ${delay}s. Time: ${DateFormatUtils.formatCurrentDateTime()}. TimeCost: ${System.currentTimeMillis() - startTime}. ${GlobalConstants.PS1.DEFAULT}<--")
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
    * @param debug
    * true：打印运行日志
    * false：不打印运行日志
    */
  def runAsSchedule(threadPoolSchedule: ScheduledExecutorService, fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1, debug: Boolean = false): Unit = {
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
        val startTime = System.currentTimeMillis()
        fun
        if (debug) println(s"--> ${GlobalConstants.PS1.GREEN}Loop invoke ${fun.getClass.getName} as ${Thread.currentThread().getName}. Delay is ${period}${timeUnit.name()}. Time: ${DateFormatUtils.formatCurrentDateTime()}. TimeCost: ${System.currentTimeMillis() - startTime}. ${GlobalConstants.PS1.DEFAULT}<--")
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
    * @param debug
    * true：打印运行日志
    * false：不打印运行日志
    */
  def runAsScheduleAtFixedRate(threadPoolSchedule: ScheduledExecutorService, fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1, debug: Boolean = false): Unit = {
    this.runAsSchedule(threadPoolSchedule, fun, initialDelay, period, true, timeUnit, threadCount, debug)
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
    * @param debug
    * true：打印运行日志
    * false：不打印运行日志
    */
  def runAsScheduleWithFixedDelay(threadPoolSchedule: ScheduledExecutorService, fun: => Unit, initialDelay: Long, period: Long, rate: Boolean = true, timeUnit: TimeUnit = TimeUnit.MINUTES, threadCount: Int = 1, debug: Boolean = false): Unit = {
    this.runAsSchedule(threadPoolSchedule, fun, initialDelay, period, false, timeUnit, threadCount, debug)
  }
}
