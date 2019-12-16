package com.zto.fire.common.acc

import java.util.concurrent.ConcurrentLinkedQueue

import com.zto.fire.common.util.GlobalConstants.{DefaultVals, PropKeys}
import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.AccumulatorV2

/**
 * 运行时累加器，用于收集运行时的jvm、gc、thread、cpu、memory、disk等信息
 *
 * @author ChengLong 2019年11月6日 16:56:38
 */
class EnvironmentAccumulator extends AccumulatorV2[String, ConcurrentLinkedQueue[String]] {
  // 用于限定运行时信息最少保存量，防止当运行时信息量达到maxEnvSize时频繁的进行clear操作
  private lazy val minEnvSize = PropUtils.getInt(PropKeys.SPARK_FIRE_ACC_ENV_MIN_SIZE, DefaultVals.minEnvSize).abs
  // 用于限定运行时信息最大保存量，防止过大撑爆driver
  private lazy val maxEnvSize = PropUtils.getInt(PropKeys.SPARK_FIRE_ACC_ENV_MAX_SIZE, DefaultVals.maxEnvSize).abs
  // 用于存放运行时信息的队列
  private val envInfoQueue = new ConcurrentLinkedQueue[String]
  // 判断是否打开运行时信息累加器
  private lazy val isEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_ACC_ENABLE, true) && PropUtils.getBoolean(PropKeys.SPARK_FIRE_ACC_ENV_ENABLE, true)

  /**
    * 判断累加器是否为空
    */
  override def isZero: Boolean = this.envInfoQueue.size() == 0

  /**
    * 用于复制累加器
    */
  override def copy(): AccumulatorV2[String, ConcurrentLinkedQueue[String]] = new EnvironmentAccumulator

  /**
    * driver端执行有效，用于清空累加器
    */
  override def reset(): Unit = this.envInfoQueue.clear

  /**
    * executor端执行，用于收集运行时信息
    *
    * @param envInfo
    * 运行时信息
    */
  override def add(envInfo: String): Unit = {
    if (this.isEnable && StringUtils.isNotBlank(envInfo)) {
      this.envInfoQueue.add(envInfo)
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
      this.envInfoQueue.addAll(other.value)
      this.clear
    }
  }

  /**
    * driver端获取累加器的值
    *
    * @return
    * 收集到的日志信息
    */
  override def value: ConcurrentLinkedQueue[String] = this.envInfoQueue

  /**
    * 当日志累积量超过maxLogSize所设定的值时清理过期的日志数据
    * 直到达到minLogSize所设定的最小值，防止频繁的进行清理
    */
  def clear: Unit = {
    if (this.envInfoQueue.size() > this.maxEnvSize) {
      while (this.envInfoQueue.size() > this.minEnvSize) {
        this.envInfoQueue.poll
      }
    }
  }
}
