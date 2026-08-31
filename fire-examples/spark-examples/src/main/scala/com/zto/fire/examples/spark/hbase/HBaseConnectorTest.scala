/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.examples.spark.hbase

import com.zto.fire._
import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{HBase, HBase2}
import com.zto.fire.examples.bean.{Student, StudentProjection}
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.spark.SparkCore
import org.apache.hadoop.hbase.client.Get
import org.apache.spark.sql.Encoders

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

/**
  * 在spark中使用java 同步 api (HBaseConnector) 的方式读写hbase表
  * 注：适用于少量数据的实时读写，更轻量
  *
  * @author ChengLong 2019-5-9 09:37:25
  * @contact Fire框架技术交流群（钉钉）：35373471
  */
@HBase("fat")
@HBase2(cluster = "fat", scanPartitions = 3, storageLevel = "DISK_ONLY")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HBaseConnectorTest extends SparkCore {
  private val tableName1 = "fire_test_1"
  private val tableName2 = "fire_test_2"

  /**
    * 使用HBaseConnector插入一个集合，可以是list、set等集合
    * 但集合的类型必须为HBaseBaseBean的子类
    */
  def testHbasePutList: Unit = {
    val studentList = Student.newStudentList()
    this.fire.hbasePutList[Student](this.tableName2, studentList)
  }

  /**
    * 使用HBaseConnector插入一个rdd的数据
    * rdd的类型必须为HBaseBaseBean的子类
    */
  def testHbasePutRDD: Unit = {
    val studentList = Student.newStudentList()
    val studentRDD = this.fire.createRDD(studentList, 2)
    // 为空的字段不插入
    studentRDD.hbasePutRDD[Student](this.tableName1)
  }

  /**
    * 使用HBaseConnector插入一个DataFrame的数据
    */
  def testHBasePutDF: Unit = {
    val studentList = Student.newStudentList()
    val studentDF = this.fire.createDataFrame(studentList, classOf[Student])
    // 每个批次插100条
    studentDF.hbasePutDF[Student](this.tableName1)
  }

  /**
    * 使用HBaseConnector插入一个Dataset的数据
    * dataset的类型必须为HBaseBaseBean的子类
    */
  def testHBasePutDS: Unit = {
    val studentList = Student.newStudentList()
    val studentDS = this.fire.createDataset(studentList)(Encoders.bean(classOf[Student]))
    // 以多版本形式插入
    studentDS.hbasePutDS[Student](this.tableName2)
  }

  /**
    * 使用HBaseConnector get数据，并将结果以list方式返回
    */
  def testHbaseGetList: Unit = {
    println("===========testHbaseGetList===========")
    val rowKeys = Seq("1", "2", "3", "5", "6")
    val studentList = this.fire.hbaseGetList2[Student](this.tableName1, rowKeys)
    studentList.foreach(println)

    val getList = ListBuffer[Get]()
    rowKeys.map(rowkey => (getList += new Get(rowkey.getBytes(StandardCharsets.UTF_8))))
    // 获取多版本形式存放的记录，并获取最新的两个版本就
    val studentList2 = this.fire.hbaseGetList[Student](this.tableName1, getList)
    studentList2.foreach(println)
  }

  /**
   * 使用HBaseConnector get数据，并将结果以map方式返回
   */
  def testHbaseGetMap: Unit = {
    println("===========testHbaseGetMap===========")
    val rowKeys = Seq("1", "2", "3", "5", "6")

    // 查询全部列
    val map = this.fire.hbaseGetMap(this.tableName1, Nil, "1")
    println("-----------------单条get--------------------")
    // 输出值带family：{"info:age":"12","info:id":"1","rowKey":"1","info:length":"12.1","info:createTime":"2026-08-31 09:13:21","info:sex":"true","info:name":"admin"}
    println(JSONUtils.getScalaMapper.writeValueAsString(map))

    // 查询指定列
    val mapList = this.fire.hbaseGetMapList2(this.tableName1, qualifiers = Seq("rowkey", "id", "name"), rowKeys)
    println("------------批量get-------------")
    mapList.foreach(map => {
      // 输出值：{"info:id":"1","info:name":"admin","rowKey":"1"}
      println(JSONUtils.getScalaMapper.writeValueAsString(map))
      println("---------------------")
    })
  }

  /**
    * 使用HBaseConnector get数据，并将结果以RDD方式返回
    */
  def testHbaseGetRDD: Unit = {
    println("===========testHBaseConnectorGetRDD===========")
    val getList = Seq("1", "2", "3", "5", "6")
    val getRDD = this.fire.createRDD(getList, 2)
    // 以多版本方式get，并将结果集封装到rdd中返回
    val studentRDD = this.fire.hbaseGetRDD[Student](this.tableName1, getRDD)
    studentRDD.printEachPartition
  }

  /**
    * 使用HBaseConnector get数据，并将结果以DataFrame方式返回
    */
  def testHbaseGetDF: Unit = {
    println("===========testHBaseConnectorGetDF===========")
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.fire.createRDD(getList, 3)
    // get到的结果以dataframe形式返回
    val studentDF = this.fire.hbaseGetDF[Student](this.tableName1, getRDD)
    studentDF.show(100, false)
  }

  /**
    * 使用HBaseConnector get数据，并将结果以Dataset方式返回
    */
  def testHBaseGetDS: Unit = {
    println("===========testHBaseGetDS===========")
    val getList = Seq("1", "2", "3", "4", "5", "6")
    val getRDD = this.fire.createRDD(getList, 2)
    // 指定在多版本获取时只取最新的两个版本
    val studentDS = this.fire.hbaseGetDS[Student](this.tableName1, getRDD)
    studentDS.show(100, false)
  }

  /**
    * 使用HBaseConnector scan数据，并以list方式返回
    */
  def testHbaseScanList: Unit = {
    println("===========testHbaseScanList===========")
    val list = this.fire.hbaseScanList2[Student](this.tableName1, "1", "6")
    list.foreach(println)
  }

  /**
   * 使用HBaseConnector scan数据，并以list[map]方式返回
   */
  def testHbaseScanMapList: Unit = {
    println("===========testHbaseScanMapList===========")
    // qualifiers 传参Nil或null表示查询所有列
    val mapList = this.fire.hbaseScanMapList2(this.tableName1, qualifiers = null, "1", "6")

    println("------------批量scan-------------")
    mapList.foreach(map => {
      // 输出值：{"info:id":"1","info:name":"admin","rowKey":"1"}
      println(JSONUtils.getScalaMapper.writeValueAsString(map))
      println("---------------------")
    })
  }

  /**
    * 使用HBaseConnector scan数据，并以RDD方式返回
    */
  def testHbaseScanRDD: Unit = {
    println("===========testHbaseScanRDD===========")
    val rdd = this.fire.hbaseScanRDD2[Student](this.tableName1, "1", "6")
    println("testHbaseScanRDD.rdd.count=" + rdd.count())
    rdd.repartition(3).collect().foreach(t => JSONUtils.toJSONString(t))
  }

  /**
    * 使用HBaseConnector scan数据，并以DataFrame方式返回
    */
  def testHbaseScanDF: Unit = {
    println("===========testHbaseScanDF===========")
    val dataFrame = this.fire.hbaseScanDF2[Student](this.tableName1, "1", "6")
    println("dataFrame.rdd.count=" + dataFrame.count())
    dataFrame.repartition(3).show(100, false)
  }

  /**
    * 使用HBaseConnector scan数据，并以DataFrame方式返回
    */
  def testHbaseScanDS: Unit = {
    println("===========testHbaseScanDF===========")
    val dataSet = this.fire.hbaseScanDS2[Student](this.tableName1, "1", "6")
    dataSet.show(100, false)
  }

  /**
    * 根据指定的rowKey list，批量删除指定的记录
    */
  def testHbaseDeleteList: Unit = {
    val rowKeyList = Seq(1.toString, 2.toString, 5.toString, 8.toString)
    this.fire.hbaseDeleteList(this.tableName1, rowKeyList)
  }

  /**
    * 根据指定的rowKey rdd，批量删除指定的记录
    */
  def testHBaseDeleteRDD: Unit = {
    val rowKeyList = Seq(1.toString, 2.toString, 3.toString, 4.toString, 5.toString, 6.toString, 7.toString, 8.toString, 9.toString, 10.toString)
    val rowKeyRDD = this.fire.createRDD(rowKeyList, 2)
    rowKeyRDD.hbaseDeleteRDD(this.tableName1)
  }

  /**
    * 根据指定的rowKey dataset，批量删除指定的记录
    */
  def testHbaseDeleteDS: Unit = {
    val rowKeyList = Seq(1.toString, 2.toString, 5.toString, 8.toString)
    val rowKeyDS = this.fire.createDataset(rowKeyList)(Encoders.STRING)
    rowKeyDS.hbaseDeleteDS(this.tableName1)
  }

  /**
   * Bean 投影查询：以 Student 写入全部列，以 StudentProjection（@HConfig(projection=true)）只查部分列
   * StudentProjection 有效字段：id、name、createTime；age 标记 @FieldName(disuse=true) 不参与投影
   * 注：getList/scanList 走 Driver 侧 Java API；getRDD/scanDF 走 Spark 分布式，需 executor 能连 HBase
   */
  def testHbaseProjection: Unit = {
    println("===========testHbaseProjection===========")
    val rowKeys = Seq("1", "2", "3", "5", "6")

    // 对照：全列 get（Student，projection=false）
    println("------------全列 get[Student]-------------")
    this.fire.hbaseGetList2[Student](this.tableName1, rowKeys).foreach(println)

    // 投影 get：仅拉 id/name/createTime
    println("------------投影 get[StudentProjection]-------------")
    this.fire.hbaseGetList2[StudentProjection](this.tableName1, rowKeys).foreach(println)

    // 投影 scan
    println("------------投影 scan[StudentProjection]-------------")
    this.fire.hbaseScanList2[StudentProjection](this.tableName1, "1", "6").foreach(println)
  }

  /**
   * 多版本get与scan
   */
  def testMutiVersion: Unit = {
    this.testHBasePutDF
    this.testHBasePutDF
    this.testHBasePutDF
    this.testHBasePutDF
    print("======testHbaseGetList=======")
    this.testHbaseGetList
    print("======testHbaseGetRDD=======")
    this.testHbaseGetRDD
    print("======testHbaseGetDF=======")
    this.testHbaseGetDF
    print("======testHBaseGetDS=======")
    this.testHBaseGetDS

    println("==========scan============")
    print("======testHbaseScanList=======")
    this.testHbaseScanList
    print("======testHbaseScanRDD=======")
    this.testHbaseScanRDD
    print("======testHbaseScanDF=======")
    this.testHbaseScanDF
    print("======testHbaseScanDS=======")
    this.testHbaseScanDS
  }

  /**
    * Spark处理过程
    * 注：此方法会被自动调用
    */
  override def process: Unit = {
    // 指定是否以多版本的形式读写
    // this.testHBaseDeleteRDD
    HBaseConnector.truncateTable(this.tableName1)
    // 以 Student 写入全部列
    this.testHbasePutRDD
    this.testHbaseGetMap
    this.testHbaseScanMapList
    // 以 StudentProjection 做部分列投影查询
    this.testHbaseProjection

    this.testHbaseDeleteDS
    HBaseConnector.truncateTable(this.tableName1)
    HBaseConnector.truncateTable(this.tableName2, keyNum = 2)

    this.testHbasePutRDD
    this.testHbasePutList
    this.testHBasePutDS
    this.testHbaseGetDF
    // this.testMutiVersion
    println("=========get========")
    this.testHbaseGetList
    this.testHbaseGetRDD
    this.testHbaseGetDF
    this.testHBaseGetDS

    println("=========scan========")
    this.testHbaseScanList
    this.testHbaseScanRDD
    this.testHbaseScanDF

    Thread.currentThread().join()
  }
}
