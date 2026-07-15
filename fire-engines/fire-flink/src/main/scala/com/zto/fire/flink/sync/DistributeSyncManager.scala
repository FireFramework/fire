package com.zto.fire.flink.sync

import com.zto.fire.common.analysis.ThreadAnalysis
import com.zto.fire.common.bean.standard.StandardResult
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.ThreadPoolType
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.common.util.{JSONUtils, PropUtils, ThreadUtils}
import com.zto.fire.core.bean.{ArthasParam, TracePerformanceParam}
import com.zto.fire.core.plugin.{ArthasDynamicLauncher, TracePerformanceDynamicLauncher}
import com.zto.fire.core.rest.SystemRestful
import com.zto.fire.core.sync.{DistributeExecuteManagerHelper, SyncManager}
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
  private lazy val standardUrl = "/system/collectStandard"
  // 用于记录血缘解析运行的次数
  private lazy val lineageRunCount = new AtomicInteger()
  private lazy val lineageThread = ThreadUtils.createManagedThreadPool("LineageSyncThread", ThreadPoolType.SCHEDULED).asInstanceOf[ScheduledExecutorService]
  // 用于记录代码标准化检测结果采集运行的次数
  private lazy val standardRunCount = new AtomicInteger()
  private lazy val standardThread = ThreadUtils.createManagedThreadPool("StandardSyncThread", ThreadPoolType.SCHEDULED).asInstanceOf[ScheduledExecutorService]


  /**
   * TaskManager从JobManager准实时同步以下信息：
   * 1. 最新配置信息
   * 2. 命令：启动或停止Arthas、ByteBuddy agent服务等
   */
  def sync: Unit = {
    ThreadUtils.scheduleWithFixedDelay({
      // 先同步 JobManager 配置，再执行分布式逻辑，避免 ThreadAnalysis 等在 enable=false 时提前启动
      if (FireFlinkConf.distributeSyncEnabled) {
        val jsonConf = SystemRestful.restInvoke(this.distributeSyncUrl)
        if (!this.lastJsonConf.equals(jsonConf)) {
          if (JSONUtils.isJson(jsonConf)) {
            val distribute = JSONUtils.parseObject[DistributeBean](jsonConf)
            distribute.getModule match {
              // 同步配置信息
              case DistributeModule.CONF => this.syncConf(distribute.getJson)
              // 同步Arthas服务的命令
              case DistributeModule.ARTHAS => ArthasDynamicLauncher.command(JSONUtils.parseObject[ArthasParam](distribute.getJson))
              // 同步ByteBuddy agent服务的命令
              case DistributeModule.TRACE => TracePerformanceDynamicLauncher.command(JSONUtils.parseObject[TracePerformanceParam](distribute.getJson))
            }
          }
          this.lastJsonConf = jsonConf
        }
      }
      DistributeExecuteManagerHelper.distributeExecute
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
   * 同步TaskManager端代码标准化检测结果到JobManager端累加器中
   */
  def collectStandard: Unit = {
    standardThread.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        logInfo(s"调用接口[$standardUrl]定时代码标准化检测采集任务已启动")
        val results = FlinkStandardAccumulatorManager.getAndReset
        if (results != null && !results.isEmpty) {
          val json = JSONUtils.toJSONString(new JArrayList[StandardResult](results))
          logInfo(s"调用接口[$standardUrl]发送代码标准化检测json：$json")
          SystemRestful.restInvoke(standardUrl, json)
        }

        if (standardRunCount.incrementAndGet() > FireFrameworkConf.traceCodeStandardRunCount) {
          logInfo(s"Flink分布式代码标准化检测采集任务即将退出，总计运行：${standardRunCount.get()}次")
          standardThread.shutdown()
        }
        logInfo(s"完成Flink分布式代码标准化检测采集：${standardRunCount.get()}次")
      }
    }, FireFrameworkConf.traceCodeStandardRunInitialDelay, FireFrameworkConf.traceCodeStandardRunPeriod, TimeUnit.SECONDS)
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

