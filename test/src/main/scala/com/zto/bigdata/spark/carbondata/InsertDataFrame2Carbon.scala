package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.common.ext.BaseStructuredStreaming
import org.apache.spark.sql.SaveMode
import com.zto.bigdata.spark.common.ext.SparkExt._

/**
  * 将dataframe数据写入到carbondata表中
  * @author ChengLong 2019-3-11 14:23:29
  */
object InsertDataFrame2Carbon extends BaseStructuredStreaming {
  val tableName = "dw_sz_zto_site_senda_bills"

  def main(args: Array[String]): Unit = {
    this.init()

    val df = this.spark.table(this.tableName)
    df.write2Carbon("default", "carbon_df_table", null)

    spark.sql("select count(1) from carbon_df_table").show()
    spark.sql("select * from carbon_df_table limit 10").show()

    this.spark.stop()
  }
}
