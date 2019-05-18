package com.zto.bigdata.spark.hbase

import java.util

import com.amazonaws.services.cognitosync.model.Dataset
import com.zto.bigdata.spark.bean.Student
import com.zto.bigdata.spark.common.core.BaseSparkCore
import com.zto.bigdata.spark.common.db.HBaseOper
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.PropUtils

import scala.collection.JavaConversions

/**
  * 在spark中使用java 同步 api 的方式读写hbase表
  *
  * @author ChengLong 2019-5-9 09:37:25
  */
object HBaseJavaApiTest extends BaseSparkCore {
  private val tableName = "zto_test_senda"

  /**
    * 使用Java Api 方式对HBase多版本进行读写
    * 注：适用于Java程序
    */
  def testJavaMultiVersion(): Unit = {
    (1 to 60).foreach(x => {
      val list = new util.ArrayList[Student]()
      list.add(new Student(1L, s"root_$x", x))
      // 多版本插入，会将数据转为json存储
      HBaseOper.insertMultiVersions(this.tableName, list)
    })
    // 多版本数据读取，指定6表示读取最近6个版本，若需读取全部版本，则此参数不填
    val studentLists = HBaseOper.getMultiVersions(this.tableName, "1", classOf[Student])
    JavaConversions.asScalaBuffer(studentLists).foreach(println)
  }

  /**
    * 使用Java API方式对版本数为1的表进行读写
    * 注：适用于Java程序
    */
  def testJavaRW(): Unit = {
    val list = new util.ArrayList[Student]()
    list.add(new Student(1L, s"root1", 12))
    list.add(new Student(2L, s"root2", 22))
    // 单版本插入（hbase表版本数为1）
    HBaseOper.insert(this.tableName, list)
    // 指定rowKey读取数据
    val student = HBaseOper.get(this.tableName, "1", classOf[Student])
    println(student)
    println(HBaseOper.get(this.tableName, "2", classOf[Student]))
    println(HBaseOper.get(this.tableName, "3", classOf[Student]))
  }

  /**
    * 使用Java API的方式将rdd中的数据写入到hbase中
    */
  def testSparkWrite(): Unit = {
    // rdd数据写入到hbase中
    val studentRDD = this.spark.parallelize(JavaConversions.asScalaBuffer(Student.buildStudentList()))
    studentRDD.hbaseInsertRDD(this.tableName, classOf[Student], multiVersion = true)
    // dataFrame数据写入到hbase中
    /*val df = this.spark.createDataFrame(studentRDD, classOf[Student])
    df.hbaseInsertDF(this.tableName, classOf[Student], multiVersion = true)*/
  }

  /**
    * spark scan HBase表记录
    */
  def testSparkScan(): Unit = {
    val rdd = this.spark.hbaseScan2RDD2(this.tableName, "1", "3", classOf[Student], true, 10)
    rdd.foreach(println)
    println("===========df==========")
    val df = this.spark.hbaseScan2DF2(this.tableName, "1", "3", classOf[Student], true, 10)
    df.show(100, false)
  }

  /**
    * 将get到的一个或多个版本映射为RDD或DataFrame
    */
  def testSparkGet(): Unit = {
    val rowKeyRDD = this.spark.parallelize(Seq("3"))
    val studentDF = rowKeyRDD.hbaseGet2DF(this.tableName, classOf[Student], true, 3)
    studentDF.show(100, false)
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    // java api方式进行单版本读写（适用于java程序）
    // this.testJavaRW()
    // java api方式进行多版本表读写（适用于java程序）
    // this.testJavaMultiVersion()

    // this.testSparkWrite()
    // this.testSparkGet()
    // this.testSparkScan()
  }

  def main(args: Array[String]): Unit = {
    this.init()

    this.spark.stop()
  }
}
