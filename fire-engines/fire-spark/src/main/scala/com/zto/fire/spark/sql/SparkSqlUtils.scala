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

package com.zto.fire.spark.sql

import com.zto.fire._
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.enu.HiveTableStoredType
import com.zto.fire.common.util.Logging
import com.zto.fire.spark.util.SparkSingletonFactory
import org.apache.spark.sql.types.StructField

/**
 * Spark sql工具类
 *
 * @author ChengLong 2023-03-15 16:10:48
 * @since 2.3.5
 */
object SparkSqlUtils extends Logging {
  private lazy val fire = SparkSingletonFactory.getSparkSession

  /**
   * 获取给定表的schema信息
   *
   * @param tableName
   * 表名（dbName.tableName）
   * @return
   * StructField
   */
  def tableSchema(tableName: String): Array[StructField] = {
    if (!this.fire.tableExists(tableName)) throw new RuntimeException(s"表${tableName}不存在，无法获取schema信息")
    this.fire.table(tableName).schema.fields
  }

  /**
   * 根据表名与where条件生成查询语句
   *
   * @param tableName
   * 表名
   * @param condition
   * where后面的查询条件
   * @return
   * 查询语句
   */
  def generateQuerySQL(tableName: String, condition: String): String = {
    val fieldList = this.tableSchema(tableName).map(field => field.name).mkString(",\n")
    s"""
       |select
       |${fieldList}
       |from $tableName
       |${if (noEmpty(condition)) "where " + condition}
       |""".stripMargin
  }

  /**
   * 根据hive数仓的表名获取在数仓中的hdfs存储路径
   *
   * @param tableName
   * hive/hudi表名
   * @return
   * url path
   */
  def getTablePath(tableName: String): String = {
    if (!this.fire.tableExists(tableName)) {
      throw new IllegalArgumentException(s"Hive表${tableName}不存在，请先建表")
    }

    val metadata = this.fire.sessionState.catalog.getTableMetadata(SparkSqlParser.toSparkTableIdentifier(TableIdentifier(tableName)))
    metadata.storage.locationUri.get.toString
  }

  /**
   * 对指定的分区表进行分区修复
   *
   * @param tableName
   * hive表名
   */
  def repairHiveTable(tableName: String): Unit = {
    tryWithLog {
      this.fire.sql(s"MSCK REPAIR TABLE $tableName")
    }(this.logger, s"${tableName}分区修复完成")
  }

  /**
   * 根据schema生成hudi建表SQL语句
   *
   * @param srcTableName
   * hive表名或者Spark的临时表名
   * @param hudiTableName
   * 待创建的hudi表名
   * @return
   * SQL DDL语句
   */
  def generateCreateSQL(srcTableName: String, hudiTableName: String, partitionFiledAndType: String, tableStoredType: HiveTableStoredType = HiveTableStoredType.ORC): String = {
    requireNonEmpty(srcTableName, hudiTableName)("表名不能为空，请检查！")

    val fields = SparkSqlUtils.tableSchema(srcTableName)
    requireNonEmpty(fields)(s"表${srcTableName}的字段列表为空！")

    val fieldBuilder = new JStringBuilder

    // 添加建表语句头
    fieldBuilder
      .append("CREATE TABLE IF NOT EXISTS ")
      .append(hudiTableName).append(" (\n")

    // 遍历schema信息，生成字段列表，排除分区字段
    fields.filter(t => !partitionFiledAndType.contains(t.name)).foreach(field => {
      fieldBuilder
        .append("\t")
        .append(field.toDDL)
        .append(",\n")
    })

    val sqlBuilder = new JStringBuilder(fieldBuilder.substring(0, fieldBuilder.length() - 2))
    sqlBuilder.append("\n) ")

    // 添加分区字段
    if (noEmpty(partitionFiledAndType)) {
      sqlBuilder
        .append("PARTITIONED BY (")
        .append(partitionFiledAndType)
        .append(")\n")
    }

    // 添加hudi表相关属性描述信息
    sqlBuilder.append(tableStoredType.toString)
    sqlBuilder.toString
  }
}
