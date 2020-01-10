package com.zto.fire.demo.flink.stream

import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.ext.FlinkExt._
import org.apache.flink.api.common.accumulators.{IntCounter, LongCounter}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()//.map(json => JSON.parseObject(json, classOf[Student]))
    // dstream.flatMap(t => t.split(",")).map(t => (t, 1)).keyBy(0).timeWindow(Time.seconds(30)).sum(1).print()

    dstream.registerAcc(new IntCounter(), "myCounter").map(new RichMapFunction[String, Int] {
      override def map(value: String): Int = {
        val count = this.getRuntimeContext.getIntCounter("myCounter")
        count.add(value.toInt)
        value.toInt
      }
    }).print

    val count = this.ssc.execute("counter test").getAccumulatorResult[Int]("myCounter")
    println("累加结果：" + count)

    // this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
