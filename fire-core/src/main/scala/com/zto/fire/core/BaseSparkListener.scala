package com.zto.fire.core

import com.zto.fire.common.acc.AccumulatorManager
import org.apache.spark.scheduler._

/**
  * Spark事件监听器桥
  * Created by ChengLong on 2018-05-19.
  */
class BaseSparkListener(baseSpark: BaseSpark) extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    this.baseSpark.onStageCompleted(stageCompleted)
    // this.baseSpark.logger.wrapLogWarn(s"${stageCompleted.stageInfo.stageId} ${stageCompleted.stageInfo.name} stage完成提交")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = this.baseSpark.onStageSubmitted(stageSubmitted)

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = this.baseSpark.onTaskStart(taskStart)

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = this.baseSpark.onTaskGettingResult(taskGettingResult)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = this.baseSpark.onTaskEnd(taskEnd)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = this.baseSpark.onJobStart(jobStart)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = this.baseSpark.onJobEnd(jobEnd)

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = this.baseSpark.onEnvironmentUpdate(environmentUpdate)

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = this.baseSpark.onBlockManagerAdded(blockManagerAdded)

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = this.baseSpark.onBlockManagerRemoved(blockManagerRemoved)

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = this.baseSpark.onUnpersistRDD(unpersistRDD)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = this.baseSpark.onApplicationStart(applicationStart)

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

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = this.baseSpark.onExecutorMetricsUpdate(executorMetricsUpdate)

  /**
    * 当添加新的executor时，重新初始化内置的累加器
    */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    AccumulatorManager.executorInstances.addAndGet(1)
    this.baseSpark.onExecutorAdded(executorAdded)
    println("重新注册累加器")
    AccumulatorManager.registerAccumulators(this.baseSpark.sc, this.baseSpark.accumulatorMap)
  }

  /**
    * 当移除已有的executor时，executor数递减
    */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    AccumulatorManager.executorInstances.decrementAndGet()
    this.baseSpark.onExecutorRemoved(executorRemoved)
  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = this.baseSpark.onBlockUpdated(blockUpdated)
}
