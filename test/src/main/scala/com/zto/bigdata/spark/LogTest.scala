package com.zto.bigdata.spark

import com.zto.bigdata.spark.common.core.BaseSparkCore
import com.zto.bigdata.spark.common.util.SparkUtils

/**
  * 本示例用于演示spark中的日志，默认为INFO,可在conf.properties中的spark.log.level进行修改
  *
  * @author ChengLong 2019年3月21日17:12:36
  */
object LogTest extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    this.init()
    this.spark.sql("use tmp")
    this.spark.sql("show tables").show()
    this.logger.wrapLogDebug("------------------->" + SparkUtils.runTime(this.startTime))
    this.logger.wrapLogInfo("------------------->" + SparkUtils.runTime(this.startTime))
    this.logger.wrapLogWarn("------------------->" + SparkUtils.runTime(this.startTime))
    this.logger.wrapLogError("------------------->" + SparkUtils.runTime(this.startTime))
  }
}
