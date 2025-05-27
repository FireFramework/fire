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

package com.zto.fire.core.sql

import com.zto.fire.common.anno.Internal
import com.zto.fire.common.bean.TableIdentifier
import com.zto.fire.common.conf.FireFrameworkConf._
import com.zto.fire.common.lineage.{LineageManager, SQLLineageManager}
import com.zto.fire.common.util.{Logging, SQLUtils, ThreadUtils}
import com.zto.fire.predef._


import java.util.concurrent.{CopyOnWriteArraySet, TimeUnit}

/**
 * 用于各引擎的SQL解析
 *
 * @author ChengLong 2021-6-18 16:28:50
 * @since 2.0.0
 */
@Internal
private[fire] trait SqlParser extends Logging {
  protected[fire] lazy val hiveTableMap = new JConcurrentHashMap[String, Boolean]()
  protected[fire] lazy val paimonTableMap = new JConcurrentHashMap[String, Boolean]()
  protected lazy val buffer = new CopyOnWriteArraySet[String]()
  this.sqlParse

  /**
   * 周期性的解析SQL语句
   */
  @Internal
  protected def sqlParse: Unit = {
    if (lineageEnable) {
      ThreadUtils.scheduleWithFixedDelay({
        this.buffer.foreach(sql => {
          this.sqlParser(sql)
          LineageManager.printLog(s"定时解析SQL血缘：$sql")
        })
        this.clear
      }, lineageRunInitialDelay, lineageRunPeriod, TimeUnit.SECONDS)
    }
  }

  /**
   * 清理解析后的SQL数据
   */
  @Internal
  private[this] def clear: Unit = {
    this.buffer.clear()
  }

  /**
   * 将待解析的SQL添加到buffer中
   */
  @Internal
  def sqlParse(sql: String): Unit = {
    if (lineageEnable && noEmpty(sql)) {
      val hideSensitiveSQL = SQLUtils.hideSensitive(sql)
      if (lineageCollectSQLEnable) {
        SQLLineageManager.addStatement(hideSensitiveSQL)
        LineageManager.printLog(s"采集SQL文本：$hideSensitiveSQL")
      }
      this.buffer += hideSensitiveSQL
    }
  }

  /**
   * 用于解析给定的SQL语句
   */
  @Internal
  def sqlParser(sql: String): Unit

  /**
   * SQL语法校验
   * @param sql
   * sql statement
   * @return
   * true：校验成功 false：校验失败
   */
  @Internal
  def sqlLegal(sql: String): Boolean

  /**
   * 用于判断给定的表是否为临时表
   */
  @Internal
  def isTempView(tableIdentifier: TableIdentifier): Boolean

  /**
   * 用于判断给定的表是否为hive表
   */
  @Internal
  def isHiveTable(tableIdentifier: TableIdentifier): Boolean

  /**
   * 用于判断给定的表是否为hudi表
   */
  def isHudiTable(tableIdentifier: TableIdentifier): Boolean

  /**
   * 将库表名转为字符串
   */
  @Internal
  def tableIdentifier(dbName: String, tableName: String): String = s"$dbName.$tableName"
}
