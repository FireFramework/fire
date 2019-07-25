package com.zto.fire.core

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

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = this.baseSpark.onApplicationEnd(applicationEnd)

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = this.baseSpark.onExecutorMetricsUpdate(executorMetricsUpdate)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    this.baseSpark.onExecutorAdded(executorAdded)
    println("--------------添加新的executor")
    baseSpark.initAccumulator
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = this.baseSpark.onExecutorRemoved(executorRemoved)

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = this.baseSpark.onBlockUpdated(blockUpdated)
}
