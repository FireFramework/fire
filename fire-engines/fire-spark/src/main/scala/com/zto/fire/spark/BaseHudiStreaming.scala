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

package com.zto.fire.spark

import com.zto.fire._
import com.zto.fire.common.anno.Scheduled
import com.zto.fire.common.util.DateFormatUtils
import com.zto.fire.spark.sql.SparkSqlUtils
import org.apache.spark.rdd.RDD

/**
 * 通用的Spark Streaming实时入湖父类
 *
 * @author ChengLong 2023-05-22 09:30:03
 * @since 2.3.5
 */
trait BaseHudiStreaming extends SparkStreaming {
  protected lazy val tableName = this.conf.get[String]("hudi.tableName")
  protected lazy val primaryKey = this.conf.get[String]("hudi.primaryKey")
  protected lazy val precombineKey = this.conf.get[String]("hudi.precombineKey")
  protected lazy val partitionFieldName = this.conf.get[String]("hudi.partitionFieldName", Some("ds"))
  protected lazy val sqlStatement = this.conf.get[String]("hudi.sql")
  protected lazy val repartition = this.conf.get[Int]("hudi.repartition", Some(-1))
  protected lazy val sink = this.conf.get[Boolean]("hudi.sink", Some(true))
  protected lazy val tmpView = this.conf.get[String]("hudi.tmpView", Some("msg_view"))
  protected lazy val retryOnFailure = this.conf.get[Int]("hudi.retry.onFailure", Some(1))

  override def before(args: Array[JString]): Unit = {
    this.conf.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    this.conf.setProperty("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
  }

  /**
   * 检查必选的配置是否合法
   */
  protected def validate(): Unit = {
    requireNonEmpty(tableName)("hudi表名不能为空，请通过hudi.tableName指定该配置")
    requireNonEmpty(primaryKey)("hudi表主键不能为空，请通过hudi.primaryKey指定该配置")
    requireNonEmpty(precombineKey)("hudi表预聚合字段不能为空，请通过hudi.precombineKey指定该配置")
    requireNonEmpty(sqlUpsert(this.tmpView))("解析临时表的SQL语句不能为空，请通过hudi.sql指定该配置")
    logInfo(
      s"""
        |-----------------hudi参数-----------------
        |hudi表名(hudi.tableName)                 :$tableName
        |hudi表主键（hudi.primaryKey）          　 :$primaryKey
        |hudi表预聚合字段(hudi.precombineKey)      :$precombineKey
        |hudi表分区字段(hudi.partitionFieldName）  :$partitionFieldName
        |消息视图名称(hudi.tmpView)                :$tmpView
        |解析kafka的sql(hudi.sql)                 :\n ${sqlUpsert(tmpView)}
        |-----------------------------------------
        |""".stripMargin)
  }

  /**
   * 数据插入前执行前置SQL语句，可以是create table语句等
   * @param tableName
   * hudi表名
   * @return
   * SQL语句
   */
  protected def sqlBefore(tableName: String): String = ""

  /**
   * 待执行的SQL语句，子类应当覆盖该方法
   * 用于解析kafka中的消息，并生成与hudi表相对于的DataFrame
   */
  protected def sqlUpsert(tmpView: String): String = this.sqlStatement

  /**
   * 每个批次执行后执行的后置SQL语句，可以是delete、update、merge等SQL
   * @param tmpView
   * 临时表名
   */
  protected def sqlAfter(tmpView: String): String = ""
}
