package com.zto.fire.examples.spark

import com.zto.fire._
import com.zto.fire.spark.BaseSparkStreaming

object Test extends BaseSparkStreaming {
  val key = "fire.partitions"

  override def process: Unit = {
    val rdd = this.spark.parallelize(1 to 10)
    println(rdd.count())
    val map = new JHashMap[String, Int]()
    map.put("1", 1)
    map.foreach(println)
    requireNonEmpty(map)
  }


  def main(args: Array[String]): Unit = {
     this.init(10, false)
  }
}
