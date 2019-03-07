package com.zto.bigdata.spark.common.acc

import java.util.Date

import com.zto.bigdata.spark.common.util.{DateFormatUtils, GlobalConstants}
import org.apache.spark.{Accumulator, AccumulatorParam}

import scala.collection.mutable.Map
import scala.collection.{SortedMap, mutable}

/**
  * 自定义多值日期累加器
  * Created by ChengLong on 2018-05-15.
  */
object MultiDateTimeAccumulators extends AccumulatorParam[Map[String, Long]] {

  /**
    * 累加数据处理逻辑
    *
    * @param map1
    * 原始累加数据
    * @param map2
    * 新进数据
    * @return
    * 累加后数据
    */
  override def addInPlace(map1: Map[String, Long], map2: Map[String, Long]): Map[String, Long] = {
    val map = Map[String, Long]()
    val keySet = map1.keySet ++ map2.keySet
    keySet.foreach(key => {
      map += (key -> (map1.getOrElse(key, 0L) + map2.getOrElse(key, 0L)))
    })

    map
  }

  /**
    * 初始化累加器
    *
    * @param initialValue
    * 初始值为空
    * @return
    */
  override def zero(initialValue: mutable.Map[String, Long]): mutable.Map[String, Long] = {
    Map[String, Long]()
  }

  /**
    * 定时打印累加器中的值，凌晨执行删除操作
    *
    * @param multiAcc
    * 多值累加器的实例
    * @param showMinutes
    * 打印间隔时长（分钟）
    * @param clear
    * 零点时分清零所有计数器
    * @param clearDay
    * 指定清空过去几天的数据，在凌晨零点执行删除（月和年的数据会一直保留）
    */
  def showMultiDateTimeAccumulators(multiAcc: Accumulator[Map[String, Long]], showMinutes: Long = 10, clear: Boolean = false, clearDay: Int = 3): Unit = {
    // 定时打印累加器中的所有值（独立线程）
    this.printTimer(multiAcc, showMinutes)
    if (clear) {
      // 定时删除累加器中过期的值（独立线程）
      this.clearTimer(multiAcc, clearDay)
    }
  }

  /**
    * 定时删除累加器中过期的值（独立线程）
    *
    * @param multiAcc
    * @param clearDay
    */
  private def clearTimer(multiAcc: Accumulator[Map[String, Long]], clearDay: Int): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        var waitZero = true
        while (true) {
          Thread.sleep(Math.abs(clearDay) * 21600000)
          while (waitZero) {
            // 等到00秒后再执行和后续操作
            if (DateFormatUtils.isZeroHour()) {
              waitZero = false
            } else {
              Thread.sleep(1000)
            }
          }
          clearAcc(multiAcc, clearDay)
          waitZero = true
        }
      }
    }).start()
  }

  /**
    * 清零指定日期之前的数据
    */
  private def clearAcc(multiAcc: Accumulator[Map[String, Long]], clearDay: Int): Unit = {
    val key = DateFormatUtils.truncate(DateFormatUtils.addDays(new Date, -Math.abs(clearDay)), GlobalConstants.Cron.DAY)
    val map = multiAcc.value
    map.foreach(t => {
      // 移除过期的小时统计数据
      if ((t._1.length - 1 - t._1.lastIndexOf("_")) > 8 && t._1.contains(key)) {
        multiAcc.value.remove(t._1)
      }
    })
    GlobalConstants.PrintModule.MULTI_ACC_DATE_TIME_START
    GlobalConstants.PrintModule.MULTI_ACC_CLEAR
    val sortedMap = SortedMap[String, Long]()
    sortedMap.++(multiAcc.value).foreach(t => {
      GlobalConstants.PrintModule.MULTI_ACC_VALUE(t)
    })
    GlobalConstants.PrintModule.MULTI_ACC_DATE_TIME_END
  }

  /**
    * 定时打印线程
    */
  private def printTimer(multiAcc: Accumulator[Map[String, Long]], showMinutes: Long): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        var waitZero = true
        while (true) {
          Thread.sleep(Math.abs(showMinutes) * 60000)
          while (waitZero) {
            // 等到00秒后再执行和后续操作
            if (Math.abs(showMinutes) == 60) {
              // 整点打印
              if (DateFormatUtils.isZeroMinute()) {
                waitZero = false
              } else {
                Thread.sleep(1000)
              }
            } else {
              // 整秒打印
              if (DateFormatUtils.isZeroSecond()) {
                waitZero = false
              } else {
                Thread.sleep(1000)
              }
            }
          }
          showAccValues(multiAcc)
          waitZero = true
        }
      }
    }).start()
  }

  /**
    * 打印累加器中的值
    */
  private def showAccValues(multiAcc: Accumulator[Map[String, Long]]): Unit = {
    GlobalConstants.PrintModule.MULTI_ACC_DATE_TIME_START
    val sortedMap = SortedMap[String, Long]()
    sortedMap.++(multiAcc.value).foreach(t => {
      GlobalConstants.PrintModule.MULTI_ACC_VALUE(t)
    })
    GlobalConstants.PrintModule.MULTI_ACC_DATE_TIME_END
  }

  /**
    * 累加给定的累加器值
    *
    * @param multiAcc
    * 多值累加器的实例
    * @param addAcc
    * 指定的累加器名称和累加的数量
    */
  def add2MultiDateTimeAccumulator(multiAcc: Accumulator[Map[String, Long]], addAcc: (String, Long)*): Unit = {
    if (multiAcc != null && addAcc != null) {
      val map = Map[String, Long]()
      addAcc.foreach(t => {
        map += (s"${t._1}_${DateFormatUtils.truncate(GlobalConstants.Cron.HOUR, true)}" -> t._2)
        map += (s"${t._1}_${DateFormatUtils.truncate(GlobalConstants.Cron.DAY, true)}" -> t._2)
        map += (s"${t._1}_${DateFormatUtils.truncate(GlobalConstants.Cron.MONTH, true)}" -> t._2)
        map += (s"${t._1}_${DateFormatUtils.truncate(GlobalConstants.Cron.YEAR, true)}" -> t._2)
      })
      multiAcc.add(map)
    }
  }

}
