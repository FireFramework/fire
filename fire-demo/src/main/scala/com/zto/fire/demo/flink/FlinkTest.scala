package com.zto.fire.demo.flink

import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.ext.FlinkExt._

object FlinkTest extends BaseFlinkStreaming {

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    this.ssc.createDirectStream().createOrReplaceTempView("test")
    this.flink.sql("select * from test").show
    this.ssc.createDirectStream(keyNum = 2).createOrReplaceTempView("test2")
    this.flink.sql("select * from test2").show

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}