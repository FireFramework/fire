package com.zto.fire.common.acc

import com.google.common.collect.HashBasedTable
import com.zto.fire.common.util.DateFormatUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.AccumulatorV2

import scala.collection.JavaConversions

/**
  * timer累加器，对相同的key进行分钟级维度累加
  *
  * @author ChengLong 2019-8-21 14:22:12
  */
class MultiTimerAccumulator extends AccumulatorV2[(String, Long), HashBasedTable[String, Long, Long]] {
  private[fire] val timerCountTable = HashBasedTable.create[String, Long, Long]

  /**
    * 用于判断当前累加器是否为空
    *
    * @return
    * true: 空 false：不为空
    */
  override def isZero: Boolean = this.timerCountTable.size() == 0

  /**
    * 用于复制一个新的累加器实例
    *
    * @return
    * 新的累加器实例对象
    */
  override def copy(): AccumulatorV2[(String, Long), HashBasedTable[String, Long, Long]] = {
    val tmpAcc = new MultiTimerAccumulator
    tmpAcc.timerCountTable.putAll(this.timerCountTable)
    tmpAcc
  }

  /**
    * 用于重置累加器
    */
  override def reset(): Unit = this.timerCountTable.clear

  /**
    * 用于添加新的数据到累加器中
    *
    * @param kv
    * 累加值的key和value
    */
  override def add(kv: (String, Long)): Unit = {
    this.mergeTable(kv._1, DateFormatUtils.formatCurrentBySchema("yyyyMMddHHmm").toLong, kv._2)
  }

  /**
    * 用于合并数据到累加器的map中
    * 存在的累加，不存在的直接添加
    *
    * @param kv
    * 累加值的key和value
    */
  private[this] def mergeTable(kv: (String, Long, Long)): Unit = {
    if (kv != null && StringUtils.isNotBlank(kv._1) && kv._2 != null && kv._3 != null) {
      val value = if (this.timerCountTable.contains(kv._1, kv._2)) this.timerCountTable.get(kv._1, kv._2) else 0L
      this.timerCountTable.put(kv._1, kv._2, kv._3 + value)
    }
  }

  /**
    * 用于合并executor端的map到driver端
    *
    * @param other
    * executor端的map
    */
  override def merge(other: AccumulatorV2[(String, Long), HashBasedTable[String, Long, Long]]): Unit = {
    val otherTable = other.value
    if (otherTable != null && otherTable.size() > 0) {
      JavaConversions.asScalaSet(otherTable.cellSet()).foreach(timer => {
        this.mergeTable(timer.getRowKey, timer.getColumnKey, timer.getValue)
      })
    }
  }

  /**
    * 用于driver端获取累加器（map）中的值
    *
    * @return
    * 累加器中的值
    */
  override def value: HashBasedTable[String, Long, Long] = this.timerCountTable
}
