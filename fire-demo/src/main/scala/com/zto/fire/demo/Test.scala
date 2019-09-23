package com.zto.fire.demo

import java.sql.ResultSet

import com.zto.fire.common.db.QueryCallback
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.Student

import scala.collection.JavaConversions

object Test extends BaseSparkStreaming {

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    val rdd = this.sc.parallelize(1 to 10, 10)
    val studentRDD = this.sc.parallelize(JavaConversions.asScalaBuffer(Student.newStudentList()), 3)
    studentRDD.createOrReplaceTempView("t_student")

    while (true) {
      println(s"==================${DateFormatUtils.formatCurrentDateTime()}==================")
      rdd.foreachPartition(i => {
        this.mark
        Thread.sleep(100)
        this.log("jdbc操作")
      })
      this.acc.addMultiCounter("multiCounter", 1)
      println("================多值累加器==============")
      JavaConversions.mapAsScalaConcurrentMap(this.acc.getMultiCounter).foreach(t => println(t._1 + " " + t._2))
      println("================多维度累加器==============")
      val size = this.acc.getMultiTimer.cellSet().size()
      JavaConversions.asScalaSet(this.acc.getMultiTimer.cellSet()).foreach(t => println(s"size=${size} 组件：" + t.getRowKey + " 时间：" + t.getColumnKey + " " + t.getValue + "条"))
      this.log("日志累加器size=" + this.acc.getLog.size())
      Thread.sleep(60000)
    }
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init(100, false)
  }
}
