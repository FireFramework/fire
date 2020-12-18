package com.zto.fire.examples.spark

import com.zto.fire.spark.BaseSparkCore

object TestHive extends BaseSparkCore {

  override def process: Unit = {
    this.spark.sql("use dim")
    this.spark.sql("show tables").show(100, false)
  }


  def main(args: Array[String]): Unit = {
     this.init(args = args)
  }
}
