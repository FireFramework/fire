package com.zto.bigdata.spark.hive

import com.zto.bigdata.spark.common.core.BaseSparkCore
import com.zto.bigdata.spark.common.util.SparkUtils

object CrossHiveClusterReader extends BaseSparkCore {
  val dfsUrl = "hdfs://192.168.25.37:8020/user/hive/warehouse/ba.db/one_two_disp_dm"

  def main(args: Array[String]): Unit = {
    this.init()
    var startTime = SparkUtils.currentTime
    val sendaDF = this.hiveContext.read.option("header", "true").option("inferSchema", "true")
      .format("orc")
      .load(this.dfsUrl)
    sendaDF.createOrReplaceTempView("tmp1")
    this.spark.sql("select count(1) from tmp1 where ds>=20190315").show()
    println(SparkUtils.runTime(startTime))

    startTime = SparkUtils.currentTime
    val sendaDF2 = this.hiveContext.read.option("header", "true")
      .option("inferSchema", "true")
      .format("orc").load(
      s"${this.dfsUrl}/ds=20190315",
      s"${this.dfsUrl}/ds=20190316",
      s"${this.dfsUrl}/ds=20190317",
      s"${this.dfsUrl}/ds=20190318",
      s"${this.dfsUrl}/ds=20190319",
      s"${this.dfsUrl}/ds=20190320",
      s"${this.dfsUrl}/ds=20190321",
      s"${this.dfsUrl}/ds=20190322"
    )
    sendaDF2.createOrReplaceTempView("tmp2")
    this.spark.sql("select count(1) from tmp2").show()
    println(SparkUtils.runTime(startTime))
  }
}
