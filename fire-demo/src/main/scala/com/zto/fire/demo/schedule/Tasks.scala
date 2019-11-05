package com.zto.fire.demo.schedule

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.util.SparkUtils

/**
 * 定时任务注册类，必须可序列化且@Scheduled标记的方法不能带参数
 *
 * @author ChengLong 2019年11月5日 17:29:35
 * @since 0.3.5
 */
class Tasks extends Serializable {

  /**
   * 只在driver端执行，不允许同一时刻同时执行该方法
   * startAt用于指定首次执行时间
   */
  @Scheduled(cron = "0/15 * * * * ?", scope = "all", concurrent = false)
  def test5: Unit = {
    println("executorId=" + SparkUtils.getExecutorId + "====方法 test5() 每15秒执行====" + DateFormatUtils.formatCurrentDateTime())
  }
}
