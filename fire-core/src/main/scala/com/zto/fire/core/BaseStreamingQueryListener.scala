package com.zto.fire.core

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * structured streaming事件监听器
 *
 * @author ChengLong 2019年12月24日 16:26:33
 * @since 0.4.1
 */
class BaseStreamingQueryListener extends StreamingQueryListener {
  @volatile protected var latestBatchId = -1L

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {

  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    /*println("Id: " + event.progress.id)
    println("batchId: " + event.progress.batchId + " msg: " + JSON.toJSONString(event.progress))
    println("输入的记录数: " + event.progress.numInputRows)
    println("每秒输入的记录数: " + event.progress.inputRowsPerSecond)
    println("每秒处理的记录数: " + event.progress.processedRowsPerSecond)
    println("sink 描述: " + event.progress.sink.description)
    println("持续的时间：" + event.progress.durationMs)
    println("start offset: " + event.progress.sources(0).startOffset)
    println("end offset: " + event.progress.sources(0).endOffset)*/
    this.latestBatchId = event.progress.batchId
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {

  }
}
