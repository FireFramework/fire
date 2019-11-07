package com.zto.fire.common.acc

import java.util.concurrent.ConcurrentLinkedQueue

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.bean.TimeCost
import com.zto.fire.common.util.GlobalConstants.{DefaultVals, PropKeys}
import com.zto.fire.common.util.PropUtils
import org.apache.spark.util.AccumulatorV2

/**
  * fire框架日志累加器
  *
  * @author ChengLong 2019-7-23 14:22:16
  */
class LogAccumulator extends AccumulatorV2[TimeCost, ConcurrentLinkedQueue[String]] {
  // 用于限定日志最少保存量，防止当日志量达到maxLogSize时频繁的进行clear操作
  private lazy val minLogSize = PropUtils.getInt(PropKeys.SPARK_FIRE_ACC_LOG_MIN_SIZE, DefaultVals.minLogSize).abs
  // 用于限定日志最大保存量，防止日志量过大，撑爆driver
  private lazy val maxLogSize = PropUtils.getInt(PropKeys.SPARK_FIRE_ACC_LOG_MAX_SIZE, DefaultVals.maxLogSize).abs
  // 用于存放日志的队列
  private val logQueue = new ConcurrentLinkedQueue[String]
  // 判断是否打开日志累加器
  private lazy val isEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_ACC_ENABLE, true) && PropUtils.getBoolean(PropKeys.SPARK_FIRE_ACC_LOG_ENABLE, true)

  /**
    * 判断累加器是否为空
    */
  override def isZero: Boolean = this.logQueue.size() == 0

  /**
    * 用于复制累加器
    */
  override def copy(): AccumulatorV2[TimeCost, ConcurrentLinkedQueue[String]] = new LogAccumulator

  /**
    * driver端执行有效，用于清空累加器
    */
  override def reset(): Unit = this.logQueue.clear

  /**
    * executor端执行，用于收集日志信息
    *
    * @param timeCost
    * 日志记录对象
    */
  override def add(timeCost: TimeCost): Unit = {
    if (this.isEnable && timeCost != null) {
      this.logQueue.add(JSON.toJSONString(timeCost, SerializerFeature.WriteNullStringAsEmpty))
      this.clear
    }
  }

  /**
    * executor端向driver端merge累加数据
    *
    * @param other
    * executor端累加结果
    */
  override def merge(other: AccumulatorV2[TimeCost, ConcurrentLinkedQueue[String]]): Unit = {
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
    if (this.logQueue.size() > this.maxLogSize) {
      while (this.logQueue.size() > this.minLogSize) {
        this.logQueue.poll
      }
    }
  }
}
