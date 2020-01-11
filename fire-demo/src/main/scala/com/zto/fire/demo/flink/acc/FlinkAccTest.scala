package com.zto.fire.demo.flink.acc

import java.util.concurrent.ConcurrentHashMap

import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.flink.core.BaseFlinkStreaming
import com.zto.fire.flink.core.acc.MultiCounterAccumulator
import com.zto.fire.flink.core.ext.FlinkExt._
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
 * flink计数器与自定义累加器的使用
 *
 * @author ChengLong 2020年1月11日 14:08:56
 * @since 0.4.1
 */
object FlinkTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    val dstream = this.ssc.createDirectStream()
    this.testFireAcc(dstream)
    val result = this.ssc.execute("multiCounter test")
    val multiCounter = result.getAccumulatorResult[ConcurrentHashMap[String, Long]]("multiCounter")
    println("多值累加器：" + multiCounter.size())


    // this.ssc.startAwaitTermination()
  }

  /**
   * flink内置计数器的使用
   */
  def testFlinkCounter(dstream: DataStream[String]): Unit = {
    dstream.setParallelism(1).registerAcc(new IntCounter(), "myCounter").map(new RichMapFunction[String, Int] {
      override def map(value: String): Int = {
        val count = this.getRuntimeContext.getIntCounter("myCounter")
        count.add(value.toInt)
        value.toInt
      }
    }).print()
  }

  def testFireAcc(dstream: DataStream[String]): Unit = {
    dstream.registerAcc(new MultiCounterAccumulator, "multiCounter").map(new RichMapFunction[String, Int] {
      override def map(value: String): Int = {
        val multiCounter = this.getRuntimeContext.getAccumulator("multiCounter").asInstanceOf[MultiCounterAccumulator]
        // multiCounter.add(DateFormatUtils.formatCurrentBySchema("yyyyMMdd HH:mm"), 1)
        multiCounter.add(value, 1)
        value.toInt
      }
    }).print()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
