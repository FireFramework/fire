package com.zto.bigdata.spark

import java.lang.management.ManagementFactory

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.bigdata.spark.common.util.GlobalConstants.PS1
import com.zto.bigdata.spark.common.util.{DateFormatUtils, GlobalConstants}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

object LocalTest {

  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder().config("hello", "world").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel(GlobalConstants.LogLevel.ERROR)
    val sc = spark.sparkContext)*/

    /*println(ManagementFactory.getOperatingSystemMXBean.getName + " " + ManagementFactory.getOperatingSystemMXBean.getVersion + " " + ManagementFactory.getOperatingSystemMXBean.getSystemLoadAverage)
    println(JSON.toJSONString(ManagementFactory.getThreadMXBean, SerializerFeature.PrettyFormat))
    println(DateFormatUtils.formatUnixDateTime(System.currentTimeMillis()))*/
    "thrall2, thrall3, thrall4, thrall5, thrall6, thrall7, thrall8, thrall9".split(",").filter(topic => StringUtils.isNotBlank(topic)).map(topic => topic.trim).toSet.foreach(println)
  }
}
