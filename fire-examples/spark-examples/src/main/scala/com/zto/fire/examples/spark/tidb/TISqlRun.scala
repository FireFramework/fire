package com.zto.fire.examples.spark.tidb

import com.zto.fire.spark.BaseSparkCore
import org.apache.spark.sql.SparkSession

/**
  * tispark整合开发
  *
  * @author ChengLong 2019-4-3 14:12:56
  */
object TISqlRun extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    this.fire = spark
    this.runAsSchedule(this.runSQL, 10, 2, true)
  }

  def runSQL: Unit = {
    spark.sql(TISql.sql).show
  }

}
