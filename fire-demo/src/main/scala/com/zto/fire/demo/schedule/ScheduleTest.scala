package com.zto.fire.demo.schedule

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.util.SparkUtils

/**
 * 用于测试定时任务
 *
 * @author ChengLong 2019年11月5日 17:27:20
 * @since 0.3.5
 */
object ScheduleTest extends BaseSparkStreaming {

  /**
   * 只在driver端执行，不允许同一时刻同时执行该方法
   * startAt用于指定首次执行时间
   */
  @Scheduled(cron = "0/5 * * * * ?", scope = "driver", concurrent = false, startAt = "2019-11-05 11:30:00")
  def test1: Unit = {
    this.log("executorId=" + SparkUtils.getExecutorId + "====方法 test1() 每5秒执行====" + DateFormatUtils.formatCurrentDateTime())
  }

  def main(args: Array[String]): Unit = {
    this.init()
    // 用于注册其他类下带有@Scheduler标记的方法
    this.registerSchedule(new Tasks)
    // 重复注册的任务会自动去重
    this.registerSchedule(new Tasks)
  }

}
