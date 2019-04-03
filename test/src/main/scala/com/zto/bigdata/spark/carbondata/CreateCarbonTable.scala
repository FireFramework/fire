package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.util.CarbondataUtils

object CreateCarbonTable {

  def main(args: Array[String]): Unit = {
    // CarbondataUtils.buildCreateStreamingTableSQL("tmp", "dw_sz_zto_site_senda_bills", classOf[Senda])
    CarbondataUtils.buildCreateTableSQL("tmp", "test2", classOf[Senda], null, true)
  }
}
