package com.zto.fire.demo

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._


object Test extends BaseSparkCore {

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    // 以子线程方式执行print方法中的逻辑
    this.runAsThread(this.print)
    Thread.currentThread().join()
  }

  /**
    * 以子线程方式执行一次
    */
  def print: Unit = {
    println("==========子线程执行===========")
  }

  def main(args: Array[String]): Unit = {
    this.init()
  }
}
