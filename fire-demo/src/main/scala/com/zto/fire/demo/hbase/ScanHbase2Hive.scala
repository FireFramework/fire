package com.zto.fire.demo.hbase

import java.util.Date

import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.demo.bean.Senda
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{CompareFilter, RegexStringComparator, RowFilter}
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel


/**
  * 扫描hbase中的数据，写入到hive中
  */
object ScanHbase2Hive extends BaseSparkCore {
  val tableName = "sz_zto_site_senda_bills"
  private val hbaseStartDateTime = DateFormatUtils.addDays(new Date, -51).substring(0, 10) + " 00:00:00"
  private val hbaseEndDateTime = DateFormatUtils.addDays(new Date, -50).substring(0, 10) + " 23:59:59"

  override def process: Unit = {
    this.spark.sql("use tmp")
    this.spark.sql("show tables").show(100, false)
    val daysFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(DateFormatUtils.getDistanceDays(this.hbaseStartDateTime, this.hbaseEndDateTime)))
    val sendaDF = this.spark.hbaseHadoopScanDF(this.tableName, new Scan().setFilter(daysFilter), classOf[Senda]).persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("count=" + sendaDF.count())
    sendaDF.show(10, false)
    sendaDF.saveAsHiveTable("tmp.test_senda_hbase", "ds", SaveMode.Overwrite)
    this.spark.sql("select * from tmp.test_senda_hbase limit 100").show(100, false)
    this.spark.sql("select count(1) from tmp.test_senda_hbase").show()
  }

  def main(args: Array[String]): Unit = {
    this.init()

    this.spark.stop()
  }
}