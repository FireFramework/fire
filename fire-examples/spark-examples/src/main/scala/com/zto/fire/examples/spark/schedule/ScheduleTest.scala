package com.zto.fire.examples.spark.schedule

import com.zto.fire._
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.spark.BaseSparkStreaming
import com.zto.fire.spark.util.SparkUtils

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
    this.logger.info("executorId=" + SparkUtils.getExecutorId + "====方法 test1() 每5秒执行====" + DateFormatUtils.formatCurrentDateTime())
  }

  /**
   * 只在driver端执行，不允许同一时刻同时执行该方法
   * startAt用于指定首次执行时间
   */
  @Scheduled(cron = "0/5 * * * * ?", scope = "all", concurrent = false)
  def test2: Unit = {
    this.logger.info("executorId=" + SparkUtils.getExecutorId + "====方法 test2() 每5秒执行====" + DateFormatUtils.formatCurrentDateTime())
  }


  // 每天凌晨4点01将锁标志设置为false，这样下一个批次就可以先更新维表再执行sql
  @Scheduled(cron = "0 1 4 * * ?")
  def updateTableJob: Unit = this.lock.compareAndSet(true, false)

  // 用于缓存变更过的维表，只有当定时任务将标记设置为可更新时才会真正拉取最新的表
  def cacheTable: Unit = {
    // 加载完成维表以后上锁
    if (this.lock.compareAndSet(false, true)) {
      this.fire.uncache("test")
      this.fire.cacheTables("test")
    }
  }

  override def process: Unit = {
    // 用于注册其他类下带有@Scheduler标记的方法
    this.registerSchedule(new Tasks)
    // 重复注册的任务会自动去重
    this.registerSchedule(new Tasks)

    // 更新并缓存维表动作，具体要根据锁的标记判断是否执行
    this.cacheTable
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }

}
