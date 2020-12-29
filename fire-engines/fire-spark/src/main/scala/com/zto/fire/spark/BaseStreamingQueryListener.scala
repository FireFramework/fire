package com.zto.fire.spark

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
      // onQueryStarted
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    this.latestBatchId = event.progress.batchId
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    // onQueryTerminated
  }
}
