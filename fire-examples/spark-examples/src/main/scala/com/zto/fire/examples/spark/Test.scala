package com.zto.fire.examples.spark

// 不论Spark或FLink，统一导入com.zto.fire._
import com.zto.fire._
import com.zto.fire.spark.BaseSparkStreaming

object Test extends BaseSparkStreaming {
  val key = "fire.partitions"

  override def process: Unit = {
    val rdd = this.spark.parallelize(1 to 10)
    println(rdd.count())
    // java相关的集合以J开头
    val map = new JHashMap[String, Int]()
    val set = new JHashSet[JInt]()
    map.put("1", 1)
    // 不需要手动转为scala map，fire会自动将java的转为scala的
    map.foreach(println)
    // 值校验，支持String/List/Map等等
    requireNonEmpty(map, "Map不能为空")
    requireNonEmpty("", "字符串不能为空")
  }


  def main(args: Array[String]): Unit = {
     this.init(10, false)
  }
}
