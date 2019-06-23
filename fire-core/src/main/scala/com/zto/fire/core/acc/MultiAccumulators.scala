package com.zto.fire.core.acc

import com.zto.fire.common.util.{DateFormatUtils, GlobalConstants}
import org.apache.spark.{Accumulator, AccumulatorParam}

import scala.collection.mutable.LinkedHashMap

/**
  * 自定义多值累加器
  * Created by ChengLong on 2018-05-10.
  */
object MultiAccumulators extends AccumulatorParam[LinkedHashMap[String, Long]] {

  /**
    * 累加值
    * @param r1
    * @param r2
    * @return
    */
  override def addInPlace(r1: LinkedHashMap[String, Long], r2: LinkedHashMap[String, Long]): LinkedHashMap[String, Long] = {
    r2.foreach(t => {
      val key = t._1
      val value = if (t._2 != null) t._2 else 0L

      if(r1.contains(key)) {
        // 如果已存在，则累加
        r1 += (key -> (r1.get(key).get + value))
      } else {
        // 不存在则新增
        r1 += (key -> value)
      }
    })

    r1
  }

  override def zero(initialValue: LinkedHashMap[String, Long]): LinkedHashMap[String, Long] = {
    LinkedHashMap[String, Long]()
  }

  /**
    * 定时打印累加器中的值
    *
    * @param multiAcc
    * 多值累加器的实例
    * @param seconds
    * 打印间隔时长（秒）
    * @param clear
    * 零点时分清零所有计数器
    * @param clearDuraion
    * 清空周期（hour/day/week/month/year）
    */
  def showMultiAccumulators(multiAcc: Accumulator[LinkedHashMap[String, Long]], seconds: Long = 60, clear: Boolean = false, clearDuraion: String = GlobalConstants.Cron.DAY): Unit = {
    if (clear) {
      if (!GlobalConstants.Cron.enumSet.contains(clearDuraion)) {
        throw new IllegalArgumentException("清空周期必须是：hour/day/week/month/year 其中的一个")
      }
    }

    // 定时打印
    this.printTimer(multiAcc, seconds, clear, clearDuraion)
  }

  /**
    * 定时打印线程
    * @param seconds
    * @param clear
    * @param clearDuraion
    */
  private def printTimer(multiAcc: Accumulator[LinkedHashMap[String, Long]], seconds: Long, clear: Boolean, clearDuraion: String): Unit = {
    new Thread(new Runnable {
      var currentDate = DateFormatUtils.formatCurrentDateTime

      override def run(): Unit = {
        var waitZero = true
        while (true) {
          Thread.sleep(seconds * 1000)
          if(seconds >= 10) {
            while (waitZero) {
              if(DateFormatUtils.isSecondDivisibleZero()) {
                waitZero = false
              } else {
                Thread.sleep(1000)
              }
            }
          }
          showAccValues(multiAcc)

          if (clear) {
            this.doClearAcc
          }
          waitZero = true
        }
      }

      /**
        * 周期性清空累加器逻辑
        */
      def doClearAcc: Unit = {
        if (GlobalConstants.Cron.HOUR.equals(clearDuraion) && !DateFormatUtils.isSameHour(currentDate, DateFormatUtils.formatCurrentDateTime)) {
          clearAcc(multiAcc)
          currentDate = DateFormatUtils.formatCurrentDateTime
        } else if (GlobalConstants.Cron.DAY.equals(clearDuraion) && !DateFormatUtils.isSameDay(currentDate, DateFormatUtils.formatCurrentDateTime)) {
          clearAcc(multiAcc)
          currentDate = DateFormatUtils.formatCurrentDateTime
        } else if (GlobalConstants.Cron.WEEK.equals(clearDuraion) && !DateFormatUtils.isSameWeek(currentDate, DateFormatUtils.formatCurrentDateTime)) {
          clearAcc(multiAcc)
          currentDate = DateFormatUtils.formatCurrentDateTime
        } else if (GlobalConstants.Cron.MONTH.equals(clearDuraion) && !DateFormatUtils.isSameMonth(currentDate, DateFormatUtils.formatCurrentDateTime)) {
          clearAcc(multiAcc)
          currentDate = DateFormatUtils.formatCurrentDateTime
        } else if (GlobalConstants.Cron.YEAR.equals(clearDuraion) && !DateFormatUtils.isSameYear(currentDate, DateFormatUtils.formatCurrentDateTime)) {
          clearAcc(multiAcc)
          currentDate = DateFormatUtils.formatCurrentDateTime
        }
      }

    }).start()
  }


  /**
    * 打印累加器中的值
    */
  private def showAccValues(multiAcc: Accumulator[LinkedHashMap[String, Long]]): Unit = {
    GlobalConstants.PrintModule.MULTI_ACC_START
    multiAcc.value.foreach(t => {
      GlobalConstants.PrintModule.MULTI_ACC_VALUE(t)
    })
    GlobalConstants.PrintModule.MULTI_ACC_END
  }

  /**
    * 清零累加器
    */
  private def clearAcc(multiAcc: Accumulator[LinkedHashMap[String, Long]]): Unit = {
    multiAcc.value.foreach(t => {
      // 清空累加器
      this.add2MultiAccumulator(multiAcc, (t._1, -t._2))
    })
    GlobalConstants.PrintModule.MULTI_ACC_START
    GlobalConstants.PrintModule.MULTI_ACC_CLEAR
    multiAcc.value.foreach(t => {
      GlobalConstants.PrintModule.MULTI_ACC_VALUE(t)
    })
    GlobalConstants.PrintModule.MULTI_ACC_END
  }

  /**
    * 累加只给定的累加器值
    *
    * @param multiAcc
    * 多值累加器的实例
    * @param addAcc
    * 指定的累加器名称和累加的数量
    */
  def add2MultiAccumulator(multiAcc: Accumulator[LinkedHashMap[String, Long]], addAcc: (String, Long)*): Unit = {
    if (multiAcc != null && addAcc != null) {
      val map = scala.collection.mutable.LinkedHashMap[String, Long]()
      map ++= addAcc
      multiAcc.add(map)
    }
  }

}
