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

package com.zto.fire.common.bean

import com.zto.fire.predef._

/**
 * 用于标识表的信息：
 * 注：在flink sql中，表的标识如果写成default_catalog.default_database.source
 * 则会忽略catalog名称与默认的数据库名称，最终解析为：source，如果数据库名称不是flink
 * 默认的default_database，则会被作为表的标识
 *
 * @author ChengLong 2022-09-06 15:19:55
 * @since 2.3.2
 */
case class TableIdentifier(private val _table: String, private val _database: String = ""/*, private val _catalog: String = ""*/) {

  lazy val table = {
    if (this._table.contains(".")) {
      val seq = this._table.split('.')
      if (seq.length == 3) {
        seq(2)
      } else if (seq.length == 2) {
        seq(1)
      } else {
        seq(0)
      }
    } else this._table
  }

  lazy val database = {
    val dbName = if (isEmpty(this._database) && this._table.contains(".")) {
      val seq = this._table.split('.')
      if (seq.length == 3) {
        seq(1)
      } else if (seq.length == 2) {
        seq(0)
      } else {
        ""
      }
    } else this._database

    // 忽略flink中的默认数据库名称，避免同一个数据源因在不同位置写了数据库名导致解析成两份
    if ("default_database".equalsIgnoreCase(dbName)) "" else dbName
  }

  lazy val catalog = {
    if (this._table.contains(".")) {
      val seq = this._table.split('.')
      if (seq.length == 3) {
        seq(0)
      } else {
        ""
      }
    } else ""
  }

  /**
   * 用于判断是否存在数据库名称
   * 如果将库名直接写到表名中，也认为库存在
   */
  def existsDB: Boolean = noEmpty(this.database) || table.contains(".")

  def notExistsDB: Boolean = !this.existsDB

  /**
   * 获取库表描述信息
   */
  def identifier: String = this.toString

  override def toString: JString = {
    val identifier = if (noEmpty(database)) {
      s"$database.$table"
    } else {
      table
    }

    identifier.trim.toLowerCase
  }

  def toNameParts: Seq[String] = {
    Seq(table, if (noEmpty(database)) database else "").filter(noEmpty(_))
  }
}

