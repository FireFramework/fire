package com.zto.fire.examples.flink.stream

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.lineage.LineageManager
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming

@Config(
  """
    |fire.lineage.enable=true
    |fire.lineage.api.enable=true
    |fire.lineage.debug.print=true
    |""")
@Streaming(10)
object RandomTest extends FlinkStreaming {
  override def process(): Unit = {
    val dstream = this.fire.createRandomLongStream(100)
    dstream.print()
    LineageManager.show(10)
  }
}
