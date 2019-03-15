package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.common.ext.BaseSparkCore
import com.zto.bigdata.spark.common.ext.SparkExt._
import org.apache.spark.sql.SaveMode

object LoadHiveTable2Carbondata extends BaseSparkCore {

  def main(args: Array[String]): Unit = {
    this.init()

    val df = this.spark.read.text("hdfs://192.168.25.37:8020/user/hive/warehouse/dim.db/baseorganize")
    df.printSchema()
    df.show(2)
    // df.write2Carbon("default", "baseorganize", null, SaveMode.Overwrite)
  }
}
