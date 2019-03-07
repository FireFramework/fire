package com.zto.bigdata.spark.util

object RegConstants {

  val sendaPattern = "zto_site_senda_bills_\\d{1,4}".r
  val sendaPatternUpper = "ZTO_SITE_SENDA_BILLS_\\d{1,4}".r

  val paifeiPattern = "zto_site_paifei_bills_\\d{1,4}".r
  val paifeiPatternUpper = "ZTO_SITE_PAIFEI_BILLS_\\d{1,4}".r

  val sendaOnePattern = "zto_site_senda_bills_one_\\d{1,4}".r
  val sendaOnePatternUpper = "ZTO_SITE_SENDA_BILLS_ONE_\\d{1,4}".r

  val paifeiOnePattern = "zto_site_paifei_bills_one_\\d{1,4}".r
  val paifeiOnePatternUpper = "ZTO_SITE_PAIFEI_BILLS_ONE_\\d{1,4}".r
}
