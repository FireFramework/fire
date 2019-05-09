package com.zto.bigdata.spark.hbase

import java.util

import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.db.HBaseOper
import com.zto.bigdata.spark.common.ext.SparkExt._

/**
  * 在spark中使用java 同步 api 的方式读写hbase表
  *
  * @author ChengLong 2019-5-9 09:37:25
  */
object HBaseJavaApiTest {
  private val tableName = "zto_test_senda"

  def main(args: Array[String]): Unit = {
    // ---------------- hbase表声明为一个版本时 ---------------- //
    val list = new util.ArrayList[Student]()
    list.add(new Student(1L, s"root", 12))
    // 单版本插入（hbase表版本数为1）
    HBaseOper.insert(this.tableName, list)
    // 指定rowKey读取数据
    val student = HBaseOper.get(this.tableName, "1")
    println(student)

    // ---------------- hbase表声明为多个版本时 ---------------- //
    (1 to 60).foreach(x => {
      val list = new util.ArrayList[Student]()
      list.add(new Student(1L, s"root_$x", x))
      // 多版本插入，会将数据转为json存储
      HBaseOper.insertMultiVersions(this.tableName, list)
    })
    // 多版本数据读取，指定6表示读取最近6个版本，若需读取全部版本，则此参数不填
    val studentLists = HBaseOper.getMultiVersions(this.tableName, 6, "1", classOf[Student]).toScalaList
    studentLists.foreach(println)
  }

}
