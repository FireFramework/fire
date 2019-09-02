package com.zto.fire.demo

import java.sql.ResultSet
import java.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.bean.rest.spark.{FunctionMeta, TableMeta}
import com.zto.fire.common.db.QueryCallback
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkStreaming
import com.zto.fire.demo.jdbc.JdbcTest.tableName
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.demo.bean.Student
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions

object Test extends BaseSparkStreaming {

  def testJdbcQuery: Unit = {
    val sql = s"select * from t_hosts where id in (?, ?, ?)"

    // 执行sql查询，并对查询结果集进行处理
    this.jdbc.executeQueryCall(sql, Seq(1, 2, 3), new QueryCallback {
      override def process(rs: ResultSet): Int = {
        var count = 0
        while (rs.next()) {
          // 对每条记录进行处理
          println("driver=> id=" + rs.getLong(1))
          count += 1
        }
        count
      }
    }, keyNum = 3)
  }

  /**
    * Spark处理逻辑
    * 注：此方法会被自动调用，不需要在main中手动调用
    */
  override def process: Unit = {
    val rdd = this.sc.parallelize(1 to 1010, 100)
    val studentRDD = this.sc.parallelize(JavaConversions.asScalaBuffer(Student.newStudentList()), 3)
    studentRDD.createOrReplaceTempView("t_student")
    while (true) {
      println(s"==================${DateFormatUtils.formatCurrentDateTime()}==================")
      rdd.foreachPartition(i => {
        this.acc.addMultiTimer("hbaseWriter", 1)
        this.acc.addMultiTimer("tidbReader", 1, "yyyy-MM-dd HH")
        // this.testJdbcQuery
      })
      val size = this.acc.getMultiTimer.cellSet().size()
      JavaConversions.asScalaSet(this.acc.getMultiTimer.cellSet()).foreach(t => println(s"size=${size} 组件：" + t.getRowKey + " 时间：" + t.getColumnKey + " " + t.getValue + "条"))
      println
      Thread.sleep(60000)
    }
    Thread.currentThread().join()
  }

  def main(args: Array[String]): Unit = {
    this.init(100, false)
  }
}
