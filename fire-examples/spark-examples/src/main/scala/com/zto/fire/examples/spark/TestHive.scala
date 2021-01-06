package com.zto.fire.examples.spark

import com.zto.fire.spark.BaseSparkCore

object TestHive extends BaseSparkCore {

  override def process: Unit = {
    this.fire.sql("use dim")
    this.fire.sql("show tables").show(100, false)
  }


  def main(args: Array[String]): Unit = {
     this.init(args = args)
  }
}
