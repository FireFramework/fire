package com.zto.fire.demo.spark

import com.zto.fire.core.BaseSparkCore

object Test extends BaseSparkCore {

  override def process: Unit = {
    val df = this.spark.sql("select * from ml.shirley_arrival_feature_summary").cache()
    println("df.count ----->" + df.count())
    println("df.show ----->" + df.show(10, false))
  }

  def main(args: Array[String]): Unit = {
    this.init()
    this.stop
  }
}
