package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala._

/**
 *
 * @author zhanggougou 2025-12-08 10:09:19
 */
@Streaming(interval = 300, unaligned = true, disableOperatorChaining = true, timeout = 180)
object FlinkCommonTest extends FlinkStreaming {

  override def process(): Unit = {
    this.fire.createRandomIntStream().map(x => x+"1111").print()
  }
}


