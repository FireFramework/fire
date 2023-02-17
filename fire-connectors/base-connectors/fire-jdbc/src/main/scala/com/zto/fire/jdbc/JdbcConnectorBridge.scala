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

package com.zto.fire.jdbc

import com.zto.fire.common.conf.KeyNum

import java.sql.{Connection, ResultSet}
import scala.reflect.ClassTag

/**
 * jdbc操作简单封装
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-05-22 15:55
 */
private[fire] trait JdbcConnectorBridge {

  /**
   * 关系型数据库插入、删除、更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param params
   * sql中的参数
   * @param connection
   * 传递已有的数据库连接
   * @param commit
   * 是否自动提交事务，默认为自动提交
   * @param closeConnection
   * 是否关闭connection，默认关闭
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 影响的记录数
   */
  def jdbcUpdate(sql: String, params: Seq[Any] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = KeyNum._1): Long = {
    JdbcConnector.update(sql, params, connection, commit, closeConnection, keyNum)
  }

  /**
   * 关系型数据库批量插入、删除、更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param paramsList
   * sql的参数列表
   * @param connection
   * 传递已有的数据库连接
   * @param commit
   * 是否自动提交事务，默认为自动提交
   * @param closeConnection
   * 是否关闭connection，默认关闭
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 影响的记录数
   */
  @deprecated("use jdbcUpdateBatch", "fire 2.3.3")
  def jdbcBatchUpdate(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = KeyNum._1): Array[Int] = {
    JdbcConnector.updateBatch(sql, paramsList, connection, commit, closeConnection, keyNum)
  }

  /**
   * 关系型数据库批量插入、删除、更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param paramsList
   * sql的参数列表
   * @param connection
   * 传递已有的数据库连接
   * @param commit
   * 是否自动提交事务，默认为自动提交
   * @param closeConnection
   * 是否关闭connection，默认关闭
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 影响的记录数
   */
  def jdbcUpdateBatch(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = KeyNum._1): Array[Int] = {
    JdbcConnector.updateBatch(sql, paramsList, connection, commit, closeConnection, keyNum)
  }

  /**
   * 执行查询操作，以JavaBean方式返回结果集
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param clazz
   * JavaBean类型
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   * @return
   * 查询结果集
   */
  def jdbcQueryList[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, keyNum: Int = KeyNum._1): List[T] = {
    JdbcConnector.queryList[T](sql, params, keyNum)
  }

  /**
   * 执行查询操作，并在QueryCallback对结果集进行处理
   *
   * @param sql
   * 查询语句
   * @param params
   * sql执行参数
   * @param callback
   * 查询回调
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   */
  def jdbcQuery[T](sql: String, params: Seq[Any] = null, callback: ResultSet => T, keyNum: Int = KeyNum._1): T = {
    JdbcConnector.query(sql, params, callback, keyNum)
  }

}
