package com.zto.fire.common.util

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.google.common.collect.EvictingQueue
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.conf.FireFrameworkConf
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions

/**
 * Fire框架异常总线，用于收集各引擎执行task过程中发生的异常信息
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-16 09:33
 */
object ExceptionBus {
  private[this] lazy val logger = LoggerFactory.getLogger(this.getClass)
  // 用于保存收集而来的异常对象
  private[this] lazy val queue = EvictingQueue.create[(Long, Throwable)](FireFrameworkConf.exceptionBusSize)
  // 队列大小，对比queue.size有性能优势
  private[fire] lazy val queueSize = new AtomicInteger(0)
  // 异常总数计数器
  private[fire] lazy val exceptionCount = new AtomicLong(0)
  // 异常发生的主机ip
  private[this] lazy val ip = SystemInfoUtils.getIp

  /**
   * 向异常总线中添加异常对象
   */
  def offer(timestamp: Long, t: Throwable): Boolean = this.synchronized {
    exceptionCount.incrementAndGet()
    this.queue.offer((timestamp, t))
  }

  /**
   * 获取并清空queue
   *
   * @return (ip, 异常集合)
   */
  @Internal
  private[fire] def getAndClear: (String, List[(Long, Throwable)]) = this.synchronized {
    val list = JavaConversions.collectionAsScalaIterable(this.queue).toList
    this.queue.clear()
    queueSize.set(0)
    this.logger.info(s"成功收集异常总线中的异常对象共计：${list.size}条，异常总线将会被清空.")
    (ip, list)
  }

  /**
   * 工具方法，用于打印异常信息
   */
  @Internal
  private[this] def offAndLogError(logger: Logger, msg: String, t: Throwable): Unit = {
    this.offer(FireUtils.currentTime, t)
    if (logger != null) logger.error(msg, t) else t.printStackTrace()
  }

  /**
   * 获取Throwable的堆栈信息
   */
  def stackTrace(t: Throwable): String = {
    if (t == null) return ""
    val stackTraceInfo = new StringBuilder()
    stackTraceInfo.append(t.toString + "\n")
    t.getStackTrace.foreach(trace => stackTraceInfo.append("\tat " + trace + "\n"))
    stackTraceInfo.toString
  }

  /**
   * 尝试执行block中的逻辑，如果出现异常，则记录日志
   *
   * @param block
   * try的具体逻辑
   * @param logger
   * 日志记录器
   * @param catchLog
   * 日志内容
   */
  def tryWithLog(block: => Unit)(logger: Logger = this.logger, catchLog: String = "执行try的过程中发生异常", isThrow: Boolean = true): Unit = {
    try {
      block
    } catch {
      case t: Throwable => {
        this.offAndLogError(logger, catchLog, t)
        if (isThrow) throw t
      }
    }
  }

  /**
   * 尝试执行block中的逻辑，如果出现异常，则记录日志，并将执行结果返回
   *
   * @param block
   * try的具体逻辑
   * @param logger
   * 日志记录器
   * @param catchLog
   * 日志内容
   */
  def tryWithReturn[T](block: => T)(logger: Logger = this.logger, catchLog: String = "执行try的过程中发生异常"): T = {
    try {
      block
    } catch {
      case t: Throwable => {
        this.offAndLogError(logger, catchLog, t)
        throw t
      }
    }
  }

  /**
   * 执行用户指定的try/catch/finally逻辑
   *
   * @param block
   * try 代码块
   * @param finallyBlock
   * finally 代码块
   * @param logger
   * 日志记录器
   * @param catchLog
   * 当执行try过程中发生异常时打印的日志内容
   * @param finallyCatchLog
   * 当执行finally代码块过程中发生异常时打印的日志内容
   */
  def tryWithFinally[T](block: => T)(finallyBlock: => Unit)(logger: Logger = this.logger, catchLog: String = "执行try的过程中发生异常", finallyCatchLog: String = "执行finally过程中发生异常"): T = {
    try {
      block
    } catch {
      case t: Throwable =>
        this.offAndLogError(logger, catchLog, t)
        throw t
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable => {
          this.offAndLogError(logger, catchLog, t)
          throw t
        }
      }
    }
  }
}
