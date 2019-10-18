package com.zto.fire.core

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.zto.fire.common.acc.AccumulatorManager
import com.zto.fire.common.enu.{JobType, ThreadPoolType}
import com.zto.fire.common.util.{DateFormatUtils, ThreadUtils}
import com.zto.fire.core.ext.SparkExt._
import org.apache.spark.Logging
import org.apache.spark.scheduler._

/**
  * Spark事件监听器桥
  * Created by ChengLong on 2018-05-19.
  */
class BaseSparkListener(baseSpark: BaseSpark) extends SparkListener with Logging {
  private[this] val module = "listener"
  private[this] val threadPool = ThreadUtils.createThreadPool("BaseSparkListener", ThreadPoolType.SCHEDULED).asInstanceOf[ScheduledExecutorService]
  private[this] val needRegister = new AtomicBoolean(false)
  // 后台周期性（每隔1分钟）检测是否需要注册新的累加器到executor端
  this.baseSpark.runAsSchedule(registerAcc, 1, 1, false, TimeUnit.MINUTES, 1, this.threadPool)

  /**
   * 当SparkContext启动时触发
   */
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    this.logFire(s"Spark 初始化完成.", this.module)
    this.baseSpark.onApplicationStart(applicationStart)
  }


  /**
   * 当Spark运行结束时执行
   */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    try {
      this.baseSpark.after()
    } finally {
      this.baseSpark.shutdown()
    }
    super.onApplicationEnd(applicationEnd)
  }

  /**
   * 当executor metrics更新时触发
   */
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = this.baseSpark.onExecutorMetricsUpdate(executorMetricsUpdate)

  /**
   * 当添加新的executor时，重新初始化内置的累加器
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    this.mark
    this.baseSpark.onExecutorAdded(executorAdded)
    if (this.baseSpark.jobType != JobType.CORE) this.needRegister.compareAndSet(false, true)
    this.logFire(s"executor[${executorAdded.executorId}] added. host: [${executorAdded.executorInfo.executorHost}].", this.module)
  }

  /**
   * 当移除已有的executor时，executor数递减
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    this.mark
    this.baseSpark.onExecutorRemoved(executorRemoved)
    this.logFire(s"executor[${executorRemoved.executorId}] removed. reason: [${executorRemoved.reason}].", this.module)
  }

  /**
   * 当环境信息更新时触发
   */
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = this.baseSpark.onEnvironmentUpdate(environmentUpdate)

  /**
   * 当BlockManager添加时触发
   */
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = this.baseSpark.onBlockManagerAdded(blockManagerAdded)

  /**
   * 当BlockManager移除时触发
   */
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = this.baseSpark.onBlockManagerRemoved(blockManagerRemoved)

  /**
   * 当block更新时触发
   */
  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = this.baseSpark.onBlockUpdated(blockUpdated)

  /**
   * 当job开始执行时触发
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = this.baseSpark.onJobStart(jobStart)

  /**
   * 当job执行完成时触发
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    this.baseSpark.onJobEnd(jobEnd)
    if (jobEnd != null && jobEnd.jobResult == JobSucceeded) {
      AccumulatorManager.addMultiTimer(module, "onJobEnd", "onJobEnd", "", "INFO", "", 1)
    } else {
      AccumulatorManager.addMultiTimer(module, "onJobEnd", "onJobEnd", "", "ERROR", "", 1)
      this.logFire(s"job failed.", this.module)
    }
  }

  /**
   * 当stage提交以后触发
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = this.baseSpark.onStageSubmitted(stageSubmitted)

  /**
   * 当stage执行完成以后触发
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    this.baseSpark.onStageCompleted(stageCompleted)
    if (stageCompleted != null && stageCompleted.stageInfo.failureReason.isEmpty) {
      AccumulatorManager.addMultiTimer(module, "onStageCompleted", "onStageCompleted", "", "INFO", "", 1)
    } else {
      AccumulatorManager.addMultiTimer(module, "onStageCompleted", "onStageCompleted", "", "ERROR", "", 1)
      this.logFire(s"stage failed. reason: " + stageCompleted.stageInfo.failureReason, this.module)
    }
  }

  /**
   * 当task开始执行时触发
   */
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = this.baseSpark.onTaskStart(taskStart)

  /**
   * 当从task获取计算结果时触发
   */
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = this.baseSpark.onTaskGettingResult(taskGettingResult)

  /**
   * 当task执行完成以后触发
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    this.baseSpark.onTaskEnd(taskEnd)
    if (taskEnd != null && taskEnd.reason != null && "Success".equalsIgnoreCase(taskEnd.reason.toString)) {
      AccumulatorManager.addMultiTimer(module, "onTaskEnd", "onTaskEnd", "", "INFO", "", 1)
    } else {
      AccumulatorManager.addMultiTimer(module, "onTaskEnd", "onTaskEnd", "", "ERROR", "", 1)
      this.logFire(s"task failed.", this.module)
    }
  }

  /**
   * 当取消缓存RDD时触发
   */
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = this.baseSpark.onUnpersistRDD(unpersistRDD)

  /**
   * 用于注册内置累加器
   */
  private[this] def registerAcc: Unit = {
    if (this.needRegister.compareAndSet(true, false)) {
      AccumulatorManager.registerAccumulators(this.baseSpark.sc)
    }
  }
}
