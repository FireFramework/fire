package com.zto.fire.demo

import com.zto.fire.core.BaseSparkCore

object LocalTest extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    this.init()
    this.spark.stop()
  }
}
