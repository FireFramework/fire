package com.zto.fire.spark.acc

import com.zto.fire.common.conf.FireFrameworkConf
import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.ConcurrentLinkedQueue

/**
  * fire框架日志累加器
  *
  * @author ChengLong 2019-7-23 14:22:16
  */
private[fire] class LogAccumulator extends AccumulatorV2[String, ConcurrentLinkedQueue[String]] {
  // 用于存放日志的队列
  private val logQueue = new ConcurrentLinkedQueue[String]
  // 判断是否打开日志累加器
  private lazy val isEnable = FireFrameworkConf.accEnable && FireFrameworkConf.accLogEnable

  /**
    * 判断累加器是否为空
    */
  override def isZero: Boolean = this.logQueue.size() == 0

  /**
    * 用于复制累加器
    */
  override def copy(): AccumulatorV2[String, ConcurrentLinkedQueue[String]] = new LogAccumulator

  /**
    * driver端执行有效，用于清空累加器
    */
  override def reset(): Unit = this.logQueue.clear

  /**
    * executor端执行，用于收集日志信息
    *
    * @param log
    * 日志信息
    */
  override def add(log: String): Unit = {
    if (this.isEnable) {
      this.logQueue.add(log)
      this.clear
    }
  }

  /**
    * executor端向driver端merge累加数据
    *
    * @param other
    * executor端累加结果
    */
  override def merge(other: AccumulatorV2[String, ConcurrentLinkedQueue[String]]): Unit = {
    if (other != null && other.value.size() > 0) {
      this.logQueue.addAll(other.value)
      this.clear
    }
  }

  /**
    * driver端获取累加器的值
    *
    * @return
    * 收集到的日志信息
    */
  override def value: ConcurrentLinkedQueue[String] = this.logQueue

  /**
    * 当日志累积量超过maxLogSize所设定的值时清理过期的日志数据
    * 直到达到minLogSize所设定的最小值，防止频繁的进行清理
    */
  def clear: Unit = {
    if (this.logQueue.size() > FireFrameworkConf.maxLogSize) {
      while (this.logQueue.size() > FireFrameworkConf.minLogSize) {
        this.logQueue.poll
      }
    }
  }
}
