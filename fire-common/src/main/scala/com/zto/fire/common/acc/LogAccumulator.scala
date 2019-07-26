package com.zto.fire.common.acc

import java.util.concurrent.ConcurrentHashMap

import com.zto.fire.common.bean.TimeCost
import org.apache.spark.util.AccumulatorV2

/**
  * fire框架日志累加器
  * @author ChengLong 2019-7-23 14:22:16
  */
class LogAccumulator extends AccumulatorV2[TimeCost, ConcurrentHashMap[String, TimeCost]] {
  private val logMap = new ConcurrentHashMap[String, TimeCost]

  override def isZero: Boolean = this.logMap.size() == 0

  override def copy(): AccumulatorV2[TimeCost, ConcurrentHashMap[String, TimeCost]] = {
    val acc = new LogAccumulator
    acc.logMap.putAll(this.logMap)
    acc
  }

  override def reset(): Unit = this.logMap.clear()

  override def add(v: TimeCost): Unit = {
    if (v != null) {
      this.logMap.put(v.getId, v)
    }
  }

  override def merge(other: AccumulatorV2[TimeCost, ConcurrentHashMap[String, TimeCost]]): Unit = {
    if (other != null && other.value.size() > 0) {
      this.logMap.putAll(other.value)
    }
  }

  override def value: ConcurrentHashMap[String, TimeCost] = {
    this.logMap
  }
}
