package com.zto.fire.common.acc

import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.AccumulatorV2

import scala.collection.JavaConversions

/**
  * 多值累加器
  *
  * @author ChengLong 2019-8-16 16:56:06
  */
class MultiCounterAccumulator extends AccumulatorV2[(String, Long), ConcurrentHashMap[String, Long]] {
  private[this] val multiCounter = new ConcurrentHashMap[String, Long]()

  override def isZero: Boolean = this.multiCounter.size() == 0

  override def copy(): AccumulatorV2[(String, Long), ConcurrentHashMap[String, Long]] = new MultiCounterAccumulator

  override def reset(): Unit = this.multiCounter.clear

  override def add(kv: (String, Long)): Unit = {
    this.mergeMap(kv)
  }

  private[this] def mergeMap(kv: (String, Long)): Unit = {
    if (kv != null && StringUtils.isNotBlank(kv._1) && kv._2 != null) {
      val sumValue = if (this.multiCounter.contains(kv._1)) {
        this.multiCounter.get(kv._1) + kv._2
      } else {
        kv._2
      }
      this.multiCounter.put(kv._1, sumValue)
    }
  }

  override def merge(other: AccumulatorV2[(String, Long), ConcurrentHashMap[String, Long]]): Unit = {
    val otherMap = other.value
    if (otherMap != null && otherMap.size() > 0) {
      JavaConversions.mapAsScalaConcurrentMap(otherMap).foreach(kv => {
        this.mergeMap(kv)
      })
    }
  }

  override def value: ConcurrentHashMap[String, Long] = this.multiCounter
}
