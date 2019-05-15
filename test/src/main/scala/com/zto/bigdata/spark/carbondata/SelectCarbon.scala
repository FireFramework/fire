package com.zto.bigdata.spark.carbondata

import org.apache.spark.sql.SparkSession

object SelectCarbon {

  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession
      .builder()
      .appName("ShenzhouSync")
      .getOrCreateCarbonSession("hdfs://appcluster/user/CarbonStore")
    spark.sparkContext.setLogLevel("ERROR")

    new Thread(new Runnable {
      override def run(): Unit = {
        while(true) {
          spark.sql("select * from dw_sz_zto_site_senda_bills limit 2").show()
          spark.sql("select count(1) from dw_sz_zto_site_senda_bills").show()
          Thread.sleep(10000)
        }
      }
    }).start()*/
  }
}
