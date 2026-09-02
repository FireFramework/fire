package com.zto.fire.examples.flink.connector.hbase

import com.zto.fire.common.util.JSONUtils
import com.zto.fire.core.anno.connector.{HBase, HBase2, HBase3, Kafka}
import com.zto.fire.examples.bean.{StuTest, Student, StudentProjection}
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.hbase.HBaseConnector
import com.zto.fire._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.hadoop.hbase.client.Get

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import java.lang.{String => JString}

/**
 * flink hbase sink
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-5-25 16:32:50
 */
@HBase("fat")
@HBase2("fat") // 对应keyNum=2的Hbase集群地址
@HBase3("fat") // 对应keyNum=3的Hbase集群地址
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
// 以上注解支持别名或url两种方式如：@Hive(thrift://hive:9083)，别名映射需配置到cluster.properties中
object HBaseTest extends FlinkStreaming {
  lazy val tableName = "fire_test_1"
  lazy val tableName2 = "fire_test_2"
  lazy val tableName3 = "fire_test_3"
  lazy val tableName5 = "fire_test_5"
  lazy val tableName6 = "fire_test_6"
  lazy val tableName7 = "fire_test_7"
  lazy val tableName8 = "fire_test_8"
  lazy val tableName9 = "fire_test_9"
  lazy val tableName10 = "fire_test_10"
  lazy val tableName11 = "fire_test_11"
  lazy val tableName12 = "fire_test_12"
  lazy val tableName13 = "fire_test_13"
  lazy val tableName14 = "fire_test_14"
  lazy val tableName15 = "fire_test_15"
  lazy val tableName16 = "fire_test_16"

  /**
   * table的hbase sink
   */
  def testTableHBaseSink(stream: DataStream[Student]): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.flink.sqlQuery("select id, name, age from student group by id, name, age")
    // 方式一、自动将row转为对应的JavaBean
    // 注意：table对象上调用hbase api，需要指定泛型
    table.hbasePutTable[Student](this.tableName).setParallelism(1)
    this.fire.hbasePutTable[Student](table, this.tableName2, keyNum = 2)

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2[Student](this.tableName3)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.fire.hbasePutTable2[Student](table, this.tableName5, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * table的hbase sink
   */
  def testTableHBaseSink2(stream: DataStream[Student]): Unit = {
    val table = this.fire.sqlQuery("select id, name, age from student group by id, name, age")

    // 方式二、用户自定义取数规则，从row中创建HBaseBaseBean的子类
    table.hbasePutTable2(this.tableName6)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
    // 或者
    this.flink.hbasePutTable2(table, this.tableName7, keyNum = 2)(row => new Student(1L, row.getField(1).toString, row.getField(2).toString.toInt))
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink(stream: DataStream[Student]): Unit = {
    // 方式一、DataStream中的数据类型为HBaseBaseBean的子类
    // stream.hbasePutDS(this.tableName)
    this.fire.hbasePutDS[Student](stream, this.tableName8)

    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName9, keyNum = 2)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName10)(value => value)
  }

  /**
   * stream hbase sink
   */
  def testStreamHBaseSink2(stream: DataStream[Student]): Unit = {
    // 方式二、将value组装为HBaseBaseBean的子类，逻辑用户自定义
    stream.hbasePutDS2(this.tableName11)(value => value)
    // 或者
    this.fire.hbasePutDS2(stream, this.tableName12, keyNum = 2)(value => value)
  }

  /**
   * hbase的基本操作
   */
  def testHBase: Unit = {
    // get操作
    val getList = ListBuffer(HBaseConnector.buildGet("1"))
    val student = HBaseConnector.get[Student](this.tableName, getList, 1)
    if (student != null) println(JSONUtils.toJSONString(student))
    // scan操作
    val studentList = HBaseConnector.scan[Student](this.tableName, HBaseConnector.buildScan("0", "9"), 1)
    if (studentList != null) println(JSONUtils.toJSONString(studentList))
    // delete操作
    HBaseConnector.deleteRows(this.tableName, Seq("1"))
  }

  /**
   * 测试getMap相关api
   */
  def testGetMap(stream: DataStream[Student]): Unit = {
    val rowKey = "1"
    val qualifiers: Seq[String] = Seq("name", "age")

    stream.map(new RichMapFunction[Student, String] {
      override def map(value: Student): JString = {
        // 部分列投影查询
        val map: Map[String, String] = HBaseConnector.getMap(tableName, qualifiers, rowKey)
        val mapJson = JSONUtils.getScalaMapper.writeValueAsString(map)
        // 打印结果：基于Map查询方式打印：{"info:age":"12","info:name":"admin","rowKey":"1"}
        println("基于Map查询方式打印：" + mapJson)

        // 通过指定泛型的方式部分列投影查询
        val stu = HBaseConnector.get[StudentProjection](tableName, Seq(rowKey))
        if (stu.nonEmpty) {
          // 打印结果：基于泛型查询方式打印：{"rowKey":"1","className":"StudentProjection","id":1,"name":"admin","age":null,"createTime":null}
          println("基于泛型查询方式打印：" + stu.head)
        }
        mapJson
      }
    }).print
  }

  private def testTimestampField(): Unit = {
    HBaseConnector.truncateTable(this.tableName2)
    this.fire.setParallelism(1)
    val seq = StuTest.newStudentList()
    val stuStream = this.fire.fromCollection(seq)
    stuStream.setParallelism(1).hbasePutDS[StuTest](this.tableName2)
  }

  /**
   * 多线程并发 Put DataStream
   */
  def testStreamHBasePutAsync(stream: DataStream[Student], threadNum: Int): Unit = {
    this.fire.hbasePutDSAsync[Student](stream, this.tableName14, threadNum = threadNum)
  }

  /**
   * 多线程并发 Put Table
   */
  def testTableHBasePutAsync(stream: DataStream[Student], threadNum: Int): Unit = {
    stream.createOrReplaceTempView("student_async_df")
    val table = this.fire.sqlQuery("select id, name, age from student_async_df group by id, name, age")
    table.hbasePutTableAsync[Student](this.tableName15, threadNum = 2).setParallelism(1)
  }

  /**
   * 多线程并发 Put DataStream2
   */
  def testStreamHBasePutAsync2(stream: DataStream[Student], threadNum: Int): Unit = {
    stream.hbasePutDSAsync2(this.tableName16, threadNum = 2)(value => value)
  }

  /**
   * stream / table 多线程 sink
   */
  def testStreamHBaseSinkAsync(stream: DataStream[Student], threadNum: Int): Unit = {
    stream.hbasePutDSAsync2(this.tableName13, threadNum = threadNum)(value => value)
  }

  /**
   * table 多线程 sink
   */
  def testTableHBaseSinkAsync(stream: DataStream[Student], threadNum: Int): Unit = {
    stream.createOrReplaceTempView("student")
    val table = this.fire.sqlQuery("select id, name, age from student group by id, name, age")
    table.hbasePutTableAsync[Student](this.tableName15, threadNum = threadNum).setParallelism(1)
    table.hbasePutTableAsync2[Student](this.tableName16, keyNum = 2, threadNum = threadNum) {
      row => new Student(row.getField(0).toString.toLong, row.getField(1).toString, row.getField(2).toString.toInt)
    }
  }

  /**
   * 多线程并发 Get
   */
  def testHbaseGetListAsync(tableName: String, threadNum: Int): Unit = {
    println("===========testHbaseGetListAsync===========")
    val rowKeys = Seq("1", "2", "3", "5", "6")
    val studentList = this.fire.hbaseGetListAsync2[Student](tableName, threadNum, rowKeys)
    studentList.foreach(println)

    val getList = ListBuffer[Get]()
    rowKeys.foreach(rowKey => getList += new Get(rowKey.getBytes(StandardCharsets.UTF_8)))
    val studentList2 = this.fire.hbaseGetListAsync[Student](tableName, threadNum, getList)
    studentList2.foreach(println)
  }

  /**
   * 多线程并发 Scan
   */
  def testHbaseScanListAsync(tableName: String, threadNum: Int): Unit = {
    println("===========testHbaseScanListAsync===========")
    val list = this.fire.hbaseScanListAsync2[Student](tableName, threadNum, "1", "6")
    list.foreach(println)
  }

  /**
   * 多线程并发 GetMap
   */
  def testHbaseGetMapListAsync(tableName: String, threadNum: Int): Unit = {
    println("===========testHbaseGetMapListAsync===========")
    val rowKeys = Seq("1", "2", "3", "5", "6")
    val mapList = this.fire.hbaseGetMapListAsync2(tableName, threadNum, rowKeys)
    mapList.foreach(map => println(JSONUtils.getScalaMapper.writeValueAsString(map)))

    val getList = ListBuffer[Get]()
    rowKeys.foreach(rowKey => getList += new Get(rowKey.getBytes(StandardCharsets.UTF_8)))
    val mapList2 = this.fire.hbaseGetMapListAsync(tableName, threadNum, getList)
    mapList2.foreach(map => println(JSONUtils.getScalaMapper.writeValueAsString(map)))
  }

  /**
   * 多线程并发 ScanMap
   */
  def testHbaseScanMapListAsync(tableName: String, threadNum: Int): Unit = {
    println("===========testHbaseScanMapListAsync===========")
    val mapList = this.fire.hbaseScanMapListAsync2(tableName, threadNum, "1", "6")
    mapList.foreach(map => println(JSONUtils.getScalaMapper.writeValueAsString(map)))
  }

  /*private def studentStream(parallelism: Int = 2): DataStream[Student] = {
    this.fire.parallelize(Student.newStudentList().asScala.toSeq).setParallelism(parallelism)
  }*/

  override def process: Unit = {
    // 测试排序列字段
    this.testTimestampField()
    this.testHBase

    HBaseConnector.truncateTable(this.tableName)
    HBaseConnector.truncateTable(this.tableName3)
    HBaseConnector.truncateTable(this.tableName5)

    val stream = this.fire.createRandomLongStream(1)
      .flatMap(t => Student.newStudentList()).setParallelism(1)
    stream.addSink(t => println(t))

    this.testTableHBaseSink(stream)
    this.testStreamHBaseSink(stream)
    this.testStreamHBaseSink2(stream)
    this.testTableHBaseSink2(stream)
    this.testGetMap(stream)
  }
}
