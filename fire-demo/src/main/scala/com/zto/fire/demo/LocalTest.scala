package com.zto.fire.demo

import com.zto.fire.common.util.PropUtils
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.OrderCommon

object LocalTest {


  def main(args: Array[String]): Unit = {
    PropUtils.invokeZrcConf("com.zto.bigdata.spark.zrc.ZrcTester1", "192.168.25.180:9000")
  }
}