package com.zto.fire.examples.spark.hive

import com.zto.fire.spark.BaseSparkCore

/**
  * 读取指定集群的hive表
 *
  * @author ChengLong 2019-3-19 17:51:59
  */
object ReadHiveMetaStore extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    this.init()
    this.spark.sql("use tmp")
    this.spark.sql("show tables").show(100, false)
    this.spark.stop()
  }
}
