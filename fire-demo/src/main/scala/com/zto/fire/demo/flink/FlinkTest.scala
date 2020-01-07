package com.zto.fire.demo.flink

import com.zto.fire.core.BaseFlinkStreaming
import com.zto.fire.core.ext.FlinkExt._

object FlinkTest extends BaseFlinkStreaming {


  /**
   * 生命周期方法：用于在SparkSession初始化之前完成用户需要的动作
   * 注：该方法会在进行init之前自动被系统调用
   *
   * @param args
   * main方法参数
   */
  override def before(args: Array[String]): Unit = {
    println("===========before=============")
  }

  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {
    this.ssc.createDirectStream().createOrReplaceTempView("test")
    this.flink.sql("select * from test").show

    this.ssc.startAwaitTermination()
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}