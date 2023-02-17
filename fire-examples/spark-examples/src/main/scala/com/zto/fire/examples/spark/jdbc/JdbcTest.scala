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

package com.zto.fire.examples.spark.jdbc

import com.zto.fire._
import com.zto.fire.common.util.{DateFormatUtils, JSONUtils}
import com.zto.fire.core.anno.connector.{Jdbc, Jdbc2}
import com.zto.fire.examples.bean.Student
import com.zto.fire.jdbc.JdbcConnector
import com.zto.fire.spark.SparkCore
import com.zto.fire.spark.util.SparkUtils
import org.apache.spark.sql.SaveMode

/**
 * Spark jdbc操作
 *
 * @author ChengLong 2019-6-17 15:17:38
 * @contact Fire框架技术交流群（钉钉）：35373471
 */
@Jdbc(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "root")
@Jdbc2(url = "jdbc:mysql://mysql-server:3306/fire", username = "root", password = "root")
object JdbcTest extends SparkCore {
  lazy val tableName = "spark_test"
  lazy val tableName2 = "t_cluster_info"
  lazy val tableName3 = "t_cluster_status"

  /**
   * 使用jdbc方式对关系型数据库进行增删改操作
   */
  def testJdbcUpdate: Unit = {
    val timestamp = DateFormatUtils.formatCurrentDateTime()
    // 执行insert操作
    val insertSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1))
    // 更新配置文件中指定的第二个关系型数据库
    this.fire.jdbcUpdate(insertSql, Seq("admin", 12, timestamp, 10.0, 1), keyNum = 2)

    // 执行更新操作
    val updateSql = s"UPDATE $tableName SET name=? WHERE id=?"
    this.fire.jdbcUpdate(updateSql, Seq("root", 1))

    // 执行批量操作
    val batchSql = s"INSERT INTO $tableName (name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"

    this.fire.jdbcUpdateBatch(batchSql, Seq(Seq("spark1", 21, timestamp, 100.123, 1),
      Seq("flink2", 22, timestamp, 12.236, 0),
      Seq("flink3", 22, timestamp, 12.236, 0),
      Seq("flink4", 22, timestamp, 12.236, 0),
      Seq("flink5", 27, timestamp, 17.236, 0)))

    // 执行批量更新
    this.fire.jdbcUpdateBatch(s"update $tableName set sex=? where id=?", Seq(Seq(1, 1), Seq(2, 2), Seq(3, 3), Seq(4, 4), Seq(5, 5), Seq(6, 6)))

    // 方式一：通过this.fire方式执行delete操作
    val sql = s"DELETE FROM $tableName WHERE id=?"
    this.fire.jdbcUpdate(sql, Seq(2))
    // 方式二：通过JdbcConnector.executeUpdate

    // 同一个事务
    /*val connection = JdbcConnector.getConnection(keyNum = 2)
    this.fire.jdbcBatchUpdate("insert", connection = connection, commit = false, closeConnection = false)
    this.fire.jdbcBatchUpdate("delete", connection = connection, commit = false, closeConnection = false)
    this.fire.jdbcBatchUpdate("update", connection = connection, commit = true, closeConnection = true)*/
  }


  /**
   * 使用jdbc方式对关系型数据库进行查询操作
   */
  def testJdbcQuery: Unit = {
    val sql = s"select * from $tableName where id in (?, ?, ?)"

    // 执行sql查询，并对查询结果集进行处理
    this.fire.jdbcQuery(sql, Seq(1, 2, 3), callback = rs => {
      while (rs.next()) {
        // 对每条记录进行处理
        println("driver=> id=" + rs.getLong(1))
      }
      1
    })

    // 将查询结果集以List[JavaBean]方式返回
    val list = this.fire.jdbcQueryList[Student](sql, Seq(1, 2, 3))
    // 方式二：使用JdbcConnector
    list.foreach(x => println(JSONUtils.toJSONString(x)))

    // 将结果集封装到RDD中
    val rdd = this.fire.jdbcQueryRDD(sql, Seq(1, 2, 3))
    rdd.printEachPartition

    // 将结果集封装到DataFrame中
    val df = this.fire.jdbcQueryDF(sql, Seq(1, 2, 3))
    df.show(10, false)
  }

  /**
   * 使用spark方式对表进行数据加载操作
   */
  def testTableLoad: Unit = {
    // 一次加载整张的jdbc小表，注：大表严重不建议使用该方法
    this.fire.jdbcTableLoadAll(this.tableName).show(100, false)
    // 根据指定分区字段的上下边界分布式加载数据
    this.fire.jdbcTableLoadBound(this.tableName, "id", 1, 10, 2).show(100, false)
    val where = Array[String]("id >=1 and id <=3", "id >=6 and id <=9", "name='root'")
    // 根据指定的条件进行数据加载，条件的个数决定了load数据的并发度
    this.fire.jdbcTableLoad(tableName, where).show(100, false)
  }

  /**
   * 使用spark方式批量写入DataFrame数据到关系型数据库
   */
  def testTableSave: Unit = {
    // 批量将DataFrame数据写入到对应结构的关系型表中
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])
    // 第二个参数默认为SaveMode.Append，可以指定SaveMode.Overwrite
    df.jdbcTableSave(this.tableName, SaveMode.Overwrite)
    // 利用sparkSession方式将DataFrame数据保存到配置的第二个数据源中
    this.fire.jdbcTableSave(df, this.tableName, SaveMode.Overwrite, keyNum = 2)
  }

  /**
   * 将DataFrame数据写入到关系型数据库中
   */
  def testDataFrameSave: Unit = {
    val df = this.fire.createDataFrame(Student.newStudentList(), classOf[Student])

    val insertSql = s"INSERT INTO spark_test(name, age, createTime, length, sex) VALUES (?, ?, ?, ?, ?)"
    // 指定部分DataFrame列名作为参数，顺序要对应sql中问号占位符的顺序，batch用于指定批次大小，默认取spark.db.jdbc.batch.size配置的值
    // 可通过db.jdbc.batch.size=100配置每次commit的记录数，控制一次写入多少条。如果要区分数据源，在db.jdbc.batch.size后面加上对应keyNum的数字后缀
    df.jdbcUpdateBatch(insertSql, Seq("name", "age", "createTime", "length", "sex"))
    this.fire.jdbcTableLoadAll(this.tableName).show(100, false)


    df.createOrReplaceTempViewCache("student")
    val sqlDF = sql("select name, age, createTime from student where id>=1").repartition(1)
    // 若不指定字段，则默认传入当前DataFrame所有列，且列的顺序与sql中问号占位符顺序一致
    sqlDF.jdbcUpdateBatch("insert into spark_test(name, age, createTime) values(?, ?, ?)", keyNum = 2)
    this.fire.jdbcTableLoadAll(this.tableName, keyNum = 2).show(100, false)
    // 等同以上方式
    // this.fire.jdbcBatchUpdateDF(sqlDF, "insert into spark_test(name, age, createTime) values(?, ?, ?)")
  }

  /**
   * 在executor中执行jdbc操作
   */
  def testExecutor: Unit = {
    JdbcConnector.query(s"select id from $tableName limit 1", null, callback = _ => {
      Thread.sleep(1000)
    })
    JdbcConnector.query(s"select id from $tableName limit 1", null, callback = _ => {
    }, keyNum = 2)
    this.logger.info("driver sql执行成功")
    val rdd = this.fire.createRDD(1 to 3, 3)
    rdd.foreachPartition(it => {
      it.foreach(i => {
        JdbcConnector.query(s"select id from $tableName limit 1", null, callback = _ => {
        })
      })
      this.logger.info("sql执行成功")
    })

    this.logConf
    val rdd2 = this.fire.createRDD(1 to 3, 3)
    rdd2.foreachPartition(it => {
      it.foreach(i => {
        JdbcConnector.query(s"select id from $tableName limit 1", null, callback = _ => {
          this.logConf
          1
        }, keyNum = 2)
        this.logger.info("sql执行成功")
      })
    })
  }

  /**
   * 用于测试分布式配置
   */
  def logConf: Unit = {
    println(s"executorId=${SparkUtils.getExecutorId} hello.world=" + this.conf.getString("hello.world", "not_found"))
    println(s"executorId=${SparkUtils.getExecutorId} hello.world.flag=" + this.conf.getBoolean("hello.world.flag", false))
    println(s"executorId=${SparkUtils.getExecutorId} hello.world.flag2=" + this.conf.getBoolean("hello.world.flag", false, keyNum = 2))
  }

  override def process: Unit = {
    // 测试环境测试
    this.testJdbcUpdate
    this.testJdbcQuery
    // this.testJdbcUpdate
    /*this.testJdbcUpdate
    this.testJdbcQuery
    this.testTableLoad
    this.testTableSave
    this.testDataFrameSave*/
    // 测试配置分发
    this.testExecutor
    Thread.sleep(100000)
  }
}