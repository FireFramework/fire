package com.zto.bigdata.spark.carbondata

import com.zto.bigdata.spark.bean.Senda
import com.zto.bigdata.spark.common.util.CarbondataUtils.buildCreateStreamingTableSQL

object CreateCarbonTable {

  def main(args: Array[String]): Unit = {
    buildCreateStreamingTableSQL("tmp", "dw_sz_zto_site_senda_bills", classOf[Senda])
  }
}
