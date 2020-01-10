package com.zto.fire.demo

import com.zto.fire.core.BaseSparkCore
import com.zto.fire.demo.bean.Student
import com.zto.fire.core.ext.SparkExt._
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.util.LongAccumulator

/**
 * 用于测试scala代码
 *
 * @author ChengLong 2019-9-4 13:39:16
 */
object ScalaTest extends BaseSparkCore {

  override def process: Unit = {
    val counter = this.sc.longAccumulator
    val rdd = this.spark.parallelize(1 to 10)
    rdd.foreach(i => counter.add(i))
    println("count=" + rdd.count)
  }

  def main(args: Array[String]): Unit = {
    this.init()
    this.stop
  }

}
