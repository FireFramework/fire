package com.zto.bigdata.spark.hive

import com.zto.bigdata.spark.common.ext.BaseSparkCore

/**
  * 读取指定集群的hive表
  * @author ChengLong 2019-3-19 17:51:59
  */
object ReadHiveMetaStore extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    this.init()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("select * from ba.one_two_disp_dm limit 10").show()
    spark.sql("select count(1) from ba.one_two_disp_dm").show()
  }
}
