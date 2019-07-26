package com.zto.bigdata.spark

import com.zto.fire.common.acc.AccumulatorManager
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._

import scala.collection.JavaConversions

object Test extends BaseSparkCore {

  override def process: Unit = {
    val rdd = spark.sparkContext.parallelize(1 to 20, 20)

    rdd.foreach(i => {
      this.mark
      AccumulatorManager.addCountValue(1)
      Thread.sleep(1000)
      this.log("hello world")
    })

    JavaConversions.mapAsScalaMap(this.logAccumulator.value).foreach(t => {
      println(t._1 + " value=" + t._2)
    })
    println("执行完成: count=" + this.count.value)

    Thread.currentThread().join
  }


  /**
    * 生命周期方法，用于在SparkSession初始化之前完成用户需要的动作
    * 注：该方法会在进行init之前自动被系统调用
    *
    * @param args
    * main方法参数
    */
  override def before(args: Array[String]): Unit = {
    println("初始化")
  }

  override def after(args: Array[String]): Unit = {
    println("用户自定义资源回收")
  }

  def main(args: Array[String]): Unit = {
    this.init(args = args)
  }

}
