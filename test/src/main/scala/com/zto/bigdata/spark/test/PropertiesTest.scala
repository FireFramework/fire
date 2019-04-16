package com.zto.bigdata.spark.test

import java.util.Properties

import com.zto.bigdata.spark.common.ext.BaseSparkCore
import com.zto.bigdata.spark.common.util.{GlobalConstants, PropUtils}

/**
  * 配置文件测试
  *
  * @author ChengLong 2019-4-15 16:39:07
  */
object PropertiesTest extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    println("hbase---------> " + GlobalConstants.HBASE_NAME)
    this.init()
    println("--------------> ds = " + GlobalConstants.SparkConf.partitionName)
    println("--------------> test = " + PropUtils.getString("test"))
    println("--------------> broker = " + GlobalConstants.kafkaBrokers)
  }
}
