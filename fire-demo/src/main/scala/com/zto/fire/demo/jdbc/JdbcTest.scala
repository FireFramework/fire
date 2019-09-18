package com.zto.fire.demo.jdbc

import java.sql.ResultSet

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zto.fire.common.db.QueryCallback
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.core.BaseSparkCore
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.util.SparkUtils
import com.zto.fire.demo.bean.Student
import org.apache.spark.sql.SaveMode

/**
  * Spark jdbc操作
  *
  * @author ChengLong 2019-6-17 15:17:38
  */
object JdbcTest extends BaseSparkCore {
  val tableName = SparkUtils.getConf("tableName")

  /**
    * 使用jdbc方式对关系型数据库进行增删改操作
    */
  def testJdbcUpdate: Unit = {
    // 执行insert操作
    val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    this.spark.jdbcUpdate(insertSql, Seq("admin", 12, DateFormatUtils.formatCurrentDateTime(), 10.0, 1))
    // 更新配置文件中指定的第二个关系型数据库
    this.spark.jdbcUpdate(insertSql, Seq("admin", 12, DateFormatUtils.formatCurrentDateTime(), 10.0, 1), keyNum = 2)

    // 执行更新操作
    val updateSql = s"UPDATE $tableName SET name=? WHERE id=?"
    this.spark.jdbcUpdate(updateSql, Seq("root", 1))

    // 执行批量操作
    val batchSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    this.spark.jdbcBatchUpdate(batchSql, Seq(Seq("spark1", 21, DateFormatUtils.formatCurrentDateTime(), 100.123, 1),
      Seq("flink2", 22, DateFormatUtils.formatCurrentDateTime(), 12.236, 0),
      Seq("flink3", 22, DateFormatUtils.formatCurrentDateTime(), 12.236, 0),
      Seq("flink4", 22, DateFormatUtils.formatCurrentDateTime(), 12.236, 0),
      Seq("flink5", 27, DateFormatUtils.formatCurrentDateTime(), 17.236, 0)))

    // 方式一：通过this.spark方式执行delete操作
    val sql = s"DELETE FROM $tableName WHERE id=?"
    this.spark.jdbcUpdate(sql, Seq(2))
    // 方式二：通过JdbcOper.executeUpdate

    // 同一个事务
    /*val connection = this.jdbc.getConnection()
    this.spark.jdbcBatchUpdate("insert", connection = connection, commit = false, closeConnection = false)
    this.spark.jdbcBatchUpdate("delete", connection = connection, commit = false, closeConnection = false)
    this.spark.jdbcBatchUpdate("update", connection = connection, commit = true, closeConnection = true)*/
  }


  /**
    * 使用jdbc方式对关系型数据库进行查询操作
    */
  def testJdbcQuery: Unit = {
    val sql = s"select * from $tableName where id in (?, ?, ?)"

    // 执行sql查询，并对查询结果集进行处理
    this.spark.jdbcQueryCall(sql, Seq(1, 2, 3), new QueryCallback {
      override def process(rs: ResultSet): Int = {
        while (rs.next()) {
          // 对每条记录进行处理
          println("driver=> id=" + rs.getLong(1))
        }
        1
      }
    })

    // 将查询结果集以List[JavaBean]方式返回
    val list = this.spark.jdbcQuery(sql, Seq(1, 2, 3), classOf[Student])
    // 方式二：使用JdbcOper
    list.foreach(x => println(JSON.toJSONString(x, SerializerFeature.NotWriteRootClassName)))

    // 将结果集封装到RDD中
    val rdd = this.spark.jdbcQueryRDD(sql, Seq(1, 2, 3), classOf[Student])
    rdd.printEachPartition

    // 将结果集封装到DataFrame中
    val df = this.spark.jdbcQueryDF(sql, Seq(1, 2, 3), classOf[Student])
    df.show(10, false)

    // 将jdbc查询结果集封装到Dataset中
    val ds = this.spark.jdbcQueryDS(sql, Seq(1, 2, 3), classOf[Student])
    ds.show(10, false)
  }

  /**
    * 使用spark方式对表进行数据加载操作
    */
  def testTableLoad: Unit = {
    // 一次加载整张的jdbc小表，注：大表严重不建议使用该方法
    this.spark.jdbcTableLoadAll(this.tableName).show(100, false)
    // 根据指定分区字段的上下边界分布式加载数据
    this.spark.jdbcTableLoadBound(this.tableName, "id", 1, 10, 2).show(100, false)
    val where = Array[String]("id >=1 and id <=3", "id >=6 and id <=9", "name='root'")
    // 根据指定的条件进行数据加载，条件的个数决定了load数据的并发度
    this.spark.jdbcTableLoad(tableName, where).show(100, false)
  }

  /**
    * 使用spark方式批量写入DataFrame数据到关系型数据库
    */
  def testTableSave: Unit = {
    // 批量将DataFrame数据写入到对应结构的关系型表中
    val df = this.spark.createDataFrame(Student.newStudentList(), classOf[Student])
    // 第二个参数默认为SaveMode.Append，可以指定SaveMode.Overwrite
    df.jdbcTableSave(this.tableName, SaveMode.Overwrite)
    // 利用sparkSession方式将DataFrame数据保存到配置的第二个数据源中
    this.spark.jdbcTableSave(df, this.tableName, SaveMode.Overwrite, keyNum = 2)
  }

  /**
    * 在executor中执行jdbc操作
    */
  def testExecutor: Unit = {
    this.jdbc.executeQueryCall(s"select id from $tableName limit 1", null, new QueryCallback {
      override def process(rs: ResultSet): Int = {
        this.mark()
        Thread.sleep(1000)
        this.log(s"=============driver123 $tableName=============")
        1
      }
    }, keyNum = 3)
    this.jdbc.executeQueryCall(s"select id from $tableName limit 1", null, new QueryCallback {
      override def process(rs: ResultSet): Int = {
        this.log(s"=============driver $tableName=============")
        1
      }
    }, keyNum = 5)
    this.log("driver sql执行成功")
    val rdd = this.spark.parallelize(1 to 100, 10)
    rdd.foreachPartition(it => {
      it.foreach(i => {
        this.jdbc.executeQueryCall(s"select id from $tableName limit 1", null, new QueryCallback {
          override def process(rs: ResultSet): Int = {
            this.log("------------------------- executorId: " + SparkUtils.getExecutorId + " date:" + DateFormatUtils.formatCurrentDate())
            1
          }
        }, keyNum = 3)
      })
      this.log("sql执行成功")
    })

    val rdd2 = this.spark.parallelize(1 to 100, 10)
    rdd2.foreachPartition(it => {
      it.foreach(i => {
        this.jdbc.executeQueryCall(s"select id from $tableName limit 1", null, new QueryCallback {
          override def process(rs: ResultSet): Int = {
            this.log("------------------------- executorId: " + SparkUtils.getExecutorId + " date:" + DateFormatUtils.formatCurrentDate())
            1
          }
        }, keyNum = 5)
        this.log("sql执行成功")
      })
    })
  }

  override def process: Unit = {
    // this.testJdbcUpdate
    // this.testJdbcQuery
    // this.testTableLoad
    // this.testTableSave
    this.testExecutor
  }

  def main(args: Array[String]): Unit = {
    this.init(args = args)

    Thread.currentThread().join()
  }
}