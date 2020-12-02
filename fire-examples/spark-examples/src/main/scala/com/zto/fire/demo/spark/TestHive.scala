package com.zto.fire.demo.spark

import com.zto.fire.common.util.PropUtils
import com.zto.fire.core.{BaseSparkCore, BaseSparkStreaming}
import com.zto.fire.core.ext.SparkExt._

object TestHive extends BaseSparkCore {

  override def process: Unit = {
    this.spark.sql(
      s"""
        |select bill_code, order_code from dw.dw_order where ds between '${this.args(0)}' and '${this.args(1)}' group by bill_code, order_code
        |""".stripMargin).show()
  }


  def main(args: Array[String]): Unit = {
     this.init(args = args)
  }
}
