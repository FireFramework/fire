package com.zto.fire.flink.sync

import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.common.util.{JSONUtils, PropUtils, ThreadUtils}
import com.zto.fire.core.bean.ArthasParam
import com.zto.fire.core.plugin.ArthasDynamicLauncher
import com.zto.fire.core.rest.SystemRestful
import com.zto.fire.core.sync.SyncManager
import com.zto.fire.flink.bean.DistributeBean
import com.zto.fire.flink.conf.FireFlinkConf
import com.zto.fire.flink.enu.DistributeModule
import com.zto.fire.predef._

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

/**
 * Flink分布式数据同步管理器，用于将数据从JobManager端同步至每一个TaskManager端
 *
 * @author ChengLong 2021-11-9 13:21:39
 * @since 2.2.0
 */
private[fire] object DistributeSyncManager extends SyncManager {
  private var lastJsonConf = ""
  private lazy val distributeSyncUrl = "/system/distributeSync"
  private lazy val lineageUrl = "/system/collectLineage"
  // 用于记录血缘解析运行的次数
  private lazy val lineageRunCount = new AtomicInteger()
  private lazy val lineageThread = ThreadUtils.createThreadPool("LineageSyncThread", ThreadPoolType.SCHEDULED).asInstanceOf[ScheduledExecutorService]


  /**
   * 准实时同步最新配置信息
   */
  def sync: Unit = {
    ThreadUtils.scheduleWithFixedDelay({
      if (!FireFlinkConf.distributeSyncEnabled) return
      val jsonConf = SystemRestful.restInvoke(this.distributeSyncUrl)
      if (!this.lastJsonConf.equals(jsonConf)) {
        if (JSONUtils.isJson(jsonConf)) {
          val distribute = JSONUtils.parseObject[DistributeBean](jsonConf)
          distribute.getModule match {
            case DistributeModule.CONF => this.syncConf(distribute.getJson)
            case DistributeModule.ARTHAS => ArthasDynamicLauncher.command(JSONUtils.parseObject[ArthasParam](distribute.getJson))
          }
        }
        this.lastJsonConf = jsonConf
      }
    }, 60, 30, TimeUnit.SECONDS)
  }

  /**
   * 同步引擎各个container的信息到累加器中
   */
  def collect: Unit = {
    lineageThread.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        LineageManager.printLog(s"调用接口[$lineageUrl]定时任务已启动")
        val lineageMap = LineageManager.getDatasourceLineage
        if (noEmpty(lineageMap)) {
          val json = JSONUtils.toJSONString(lineageMap)
          LineageManager.printLog(s"调用接口[$lineageUrl]发送血缘json：$json")
          SystemRestful.restInvoke(lineageUrl, json)
        }

        if (lineageRunCount.incrementAndGet() > FireFrameworkConf.lineageRunCount) {
          logInfo(s"Flink分布式血缘解析与采集任务即将退出，总计运行：${lineageRunCount.get()}次")
          lineageThread.shutdown()
        }
        LineageManager.printLog(s"完成Flink分布式血缘解析与采集：${lineageRunCount.get()}次")
      }
    }, FireFrameworkConf.lineageRunInitialDelay, FireFrameworkConf.lineageRunPeriod, TimeUnit.SECONDS)
  }

  /**
   * 更新配置信息
   */
  def syncConf(json: String): Unit = {
    if (noEmpty(json)) {
      val confMap = JSONUtils.parseObject[JMap[String, String]](json)
      PropUtils.setProperties(confMap)
      logInfo(s"本次分布式更新配置数：${confMap.size()}个")
    }
  }
}

