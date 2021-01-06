package com.zto.fire.examples.spark.hive

import com.zto.fire._
import com.zto.fire.spark.BaseSparkCore

/**
  * 本示例用于演示spark读取不同hive集群，配置文件请见 HiveClusterReader.properties，继承自BaseSparkCore表示是一个离线的spark程序
  * 如果需要使用不同的hive集群，只需在该类同名的配置文件中加一下配置即可：hive.cluster=streaming，表示读取180实时集群的hive元数据
  *
  * @author ChengLong 2019-5-17 10:39:19
  */
object HiveClusterReader extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    // 必须调用init()方法完成sparkSession的初始化
    this.init()

    // spark为sparkSession的实例，已经在init()中完成初始化，可以直接通过this.fire或this.spark方式调用
    this.fire.sql("use tmp")
    this.fire.sql("show tables").show(100, false)

    this.fire.stop()
  }
}
