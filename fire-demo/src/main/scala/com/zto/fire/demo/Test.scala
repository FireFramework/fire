package com.zto.fire.demo

import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkStreaming

import scala.collection.JavaConversions

object Test extends BaseSparkStreaming {

  @Scheduled(fixedInterval = 1000 * 60)
  def printEnv: Unit = {
    println("runtime info: " + DateFormatUtils.formatCurrentDateTime())
    JavaConversions.asScalaIterator(this.acc.getEnv.iterator()).take(10).foreach(println)
  }

  override def process: Unit = {

  }


  def main(args: Array[String]): Unit = {
    this.init(10, false)
  }
}
