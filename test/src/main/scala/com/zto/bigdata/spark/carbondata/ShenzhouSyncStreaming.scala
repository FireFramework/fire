package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.ext.BaseSparkStreaming
import com.zto.bigdata.spark.common.ext.SparkExt._
import org.apache.spark.sql.SaveMode

/**
  * 以streaming方式写carbondata
  *
  * @author ChengLong 2019-4-2 19:17:56
  */
object ShenzhouSyncStreaming extends BaseSparkStreaming {
  val dbName = "tmp"
  val tableName = "test_senda2"

  def main(args: Array[String]): Unit = {
    this.init(30L, false)

    if (args != null && args.length > 0) {
      this.spark.dropCarbonTable(this.dbName, this.tableName)
      // this.spark.createCarbonTable(this.dbName, this.tableName, classOf[Senda])
    }

    this.runAsSchedule(this.printCount, 60 * 60, 1, true)
  }


  /**
    * Spark处理过程
    * 注：此方法会被自动调用，若需使用
    * checkpoint中的数据，则子类必须复写该方法
    */
  override def process: Unit = {
    // 默认broker、topic、groupId等信息从该类同名的配置文件中读取，比如当前类名为Shenzhou，那么默认会从Shenzhou.properties中读取配置
    // 使用该方法需导入：import com.zto.bigdata.spark.common.ext.SparkExt._
    val dstream = this.ssc.createDirectStream()
    dstream.foreachRDD((rdd, time) => {
      // this.parseJson2DataFrame(rdd, classOf[Senda]).writeStreaming2Carbon(this.dbName, tableName, time)
      // 将json数据解析成Senda对象对应的类型
      this.parseJson2DataFrame(rdd, classOf[Senda]).write2Carbon(this.dbName, tableName, null, SaveMode.Overwrite)
    })

    this.ssc.start()
    this.ssc.awaitTermination()
  }


  /**
    * 统计表中的记录数
    */
  def printCount: Unit = {
    spark.minorCompact(this.dbName, this.tableName)
  }
}
