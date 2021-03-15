package com.zto.fire.core.task

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.zto.fire.common.bean.runtime.RuntimeInfo
import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import com.zto.fire.common.util.UnitFormatUtils.DateUnitEnum
import com.zto.fire.common.util.{DatasourceManager, DateFormatUtils, EncryptUtils, HttpClientUtils, LogUtils, UnitFormatUtils}
import com.zto.fire.core.BaseFire
import com.zto.fire.predef._
import org.apache.commons.httpclient.Header
import org.slf4j.{Logger, LoggerFactory}

/**
 * Fire框架内部的定时任务
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-14 11:02
 */
private[fire] class FireInternalTask(baseFire: BaseFire) extends Serializable {
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * fire框架内部接口调用工具
   *
   * @param urlSuffix
   * 接口后缀
   * @param json
   * 请求参数
   * @return
   * 接口响应结果
   */
  protected def restInvoke(urlSuffix: String, json: String): String = {
    if (FireFrameworkConf.restEnable && noEmpty(FireFrameworkConf.fireRestUrl, urlSuffix)) {
      val restful = FireFrameworkConf.fireRestUrl + urlSuffix

      tryWithReturn {
        val secret = EncryptUtils.md5Encrypt(FireFrameworkConf.restServerSecret + this.baseFire.className + DateFormatUtils.formatCurrentDate)
        if (noEmpty(json)) {
          HttpClientUtils.doPost(restful, json, new Header("Content-Type", "application/json"), new Header("Authorization", secret))
        } else {
          HttpClientUtils.doGet(restful, new Header("Content-Type", "application/json"), new Header("Authorization", secret))
        }
      }(this.logger, "接口调用成功", "接口调用失败，请检查")
    }
    ""
  }

  /**
   * 定时采集运行时的jvm、gc、thread、cpu、memory、disk等信息
   * 并将采集到的数据存放到EnvironmentAccumulator中
   */
  def jvmMonitor: Unit = {
    val runtimeInfo = RuntimeInfo.getRuntimeInfo
    if (runtimeInfo != null && logger != null) {
      LogUtils.logStyle(this.logger, s"Jvm信息:${runtimeInfo.getIp}")(logger => {
        val jvmInfo = runtimeInfo.getJvmInfo
        val cpuInfo = runtimeInfo.getCpuInfo
        val threadInfo = runtimeInfo.getThreadInfo
        logger.info(
          s"""${FirePS1Conf.PINK}
             |GC      -> YGC: ${jvmInfo.getMinorGCCount}   YGCT: ${UnitFormatUtils.readable(jvmInfo.getMinorGCTime, UnitFormatUtils.TimeUnitEnum.MS)}    FGC: ${jvmInfo.getFullGCCount}   FGCT: ${UnitFormatUtils.readable(jvmInfo.getFullGCTime, UnitFormatUtils.TimeUnitEnum.MS)}
             |OnHeap  -> Total: ${UnitFormatUtils.readable(jvmInfo.getMemoryTotal, DateUnitEnum.BYTE)}    Used: ${UnitFormatUtils.readable(jvmInfo.getMemoryUsed, DateUnitEnum.BYTE)}   Free: ${UnitFormatUtils.readable(jvmInfo.getMemoryFree, DateUnitEnum.BYTE)}   HeapMax: ${UnitFormatUtils.readable(jvmInfo.getHeapMaxSize, DateUnitEnum.BYTE)}   HeapUsed: ${UnitFormatUtils.readable(jvmInfo.getHeapUseSize, DateUnitEnum.BYTE)}    Committed: ${UnitFormatUtils.readable(jvmInfo.getHeapCommitedSize, DateUnitEnum.BYTE)}
             |OffHeap -> Total: ${UnitFormatUtils.readable(jvmInfo.getNonHeapMaxSize, DateUnitEnum.BYTE)}   Used: ${UnitFormatUtils.readable(jvmInfo.getNonHeapUseSize, DateUnitEnum.BYTE)}   Committed: ${UnitFormatUtils.readable(jvmInfo.getNonHeapCommittedSize, DateUnitEnum.BYTE)}
             |CPUInfo -> Load: ${cpuInfo.getCpuLoad}   LoadAverage: ${cpuInfo.getLoadAverage.mkString(",")}   IoWait: ${cpuInfo.getIoWait}   IrqTick: ${cpuInfo.getIrqTick}
             |Thread  -> Total: ${threadInfo.getTotalCount}    TotalStarted: ${threadInfo.getTotalStartedCount}   Peak: ${threadInfo.getPeakCount}   Deamon: ${threadInfo.getDeamonCount}   CpuTime: ${UnitFormatUtils.readable(threadInfo.getCpuTime, UnitFormatUtils.TimeUnitEnum.MS)}    UserTime: ${UnitFormatUtils.readable(threadInfo.getUserTime, UnitFormatUtils.TimeUnitEnum.MS)} ${FirePS1Conf.DEFAULT}
             |""".stripMargin)
      })
    }
  }
}
