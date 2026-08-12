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

import com.zto.fire.common.anno.API
import com.zto.fire.common.conf.KeyNum

import java.sql.{Connection, ResultSet}
import scala.reflect.ClassTag

/**
 * Jdbc api集合
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 15:49
 */
trait JdbcFunctions {

  /**
   * 根据指定的keyNum获取对应的数据库连接
   */
  @API
  def getConnection(keyNum: Int = KeyNum._1): Connection = JdbcConnector(keyNum = keyNum).getConnection

  /**
   * 关闭指定的jdbc连接
   */
  @API
  def closeConnection(connection: Connection): Unit = {
    if (connection != null && !connection.isClosed) {
      try connection.close() catch { case _: Throwable => }
    }
  }

  /**
   * 更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param params
   * sql中的参数
   * @param connection
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
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
  @API
  @deprecated("use update", "fire 2.3.3")
  def executeUpdate(sql: String, params: Seq[Any] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = KeyNum._1): Long = {
    JdbcConnector(keyNum = keyNum).executeUpdate(sql, params, connection, commit, closeConnection)
  }

  /**
   * 更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param params
   * sql中的参数
   * @param connection
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
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
  @API
  def update(sql: String, params: Seq[Any] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = KeyNum._1): Long = {
    JdbcConnector(keyNum = keyNum).update(sql, params, connection, commit, closeConnection)
  }

  /**
   * 执行批量更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param paramsList
   * sql的参数列表
   * @param connection
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
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
  @API
  def updateBatch(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = KeyNum._1): Long = {
    JdbcConnector(keyNum = keyNum).updateBatch(sql, paramsList, connection, commit, closeConnection)
  }

  /**
   * 使用固定大小线程池并发执行批量更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param paramsList
   * sql的参数列表
   * @param commit
   * 是否自动提交事务，默认为自动提交
   * @param threadNum
   * 并发任务数，实际并发度不会超过连接池大小
   * @return
   * 影响的记录数
   */
  @API
  def updateBatchAsync(sql: String, paramsList: Seq[Seq[Any]] = null, threadNum: Int = 1, keyNum: Int = KeyNum._1): Long = {
    JdbcConnector(keyNum = keyNum).updateBatchAsync(sql, paramsList, threadNum)
  }

  /**
   * 执行批量更新操作
   *
   * @param sql
   * 待执行的sql语句
   * @param paramsList
   * sql的参数列表
   * @param connection
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
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
  @API
  @deprecated("use updateBatch", "fire 2.3.3")
  def executeBatch(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = KeyNum._1): Long = {
    JdbcConnector(keyNum = keyNum).executeBatch(sql, paramsList, connection, commit, closeConnection)
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
   */
  @API
  @deprecated("use queryList", "fire 2.3.3")
  def executeQueryList[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, keyNum: Int = KeyNum._1): List[T] = {
    JdbcConnector(keyNum = keyNum).executeQueryList(sql, params)
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
   */
  @API
  def queryList[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, keyNum: Int = KeyNum._1): List[T] = {
    JdbcConnector(keyNum = keyNum).queryList(sql, params)
  }

  /**
   * 使用固定大小线程池并发执行查询操作，并将结果合并返回
   */
  @API
  def queryListAsync[T <: Object : ClassTag](sql: String, paramsList: Seq[Seq[Any]] = null, threadNum: Int = 5, keyNum: Int = KeyNum._1): List[T] = {
    JdbcConnector(keyNum = keyNum).queryListAsync(sql, paramsList, threadNum)
  }

  /**
   * 使用固定大小线程池并发执行查询操作
   */
  @API
  def queryAsync[T](sql: String, paramsList: Seq[Seq[Any]] = null, threadNum: Int = 5, keyNum: Int = KeyNum._1)(callback: ResultSet => T): List[T] = {
    JdbcConnector(keyNum = keyNum).queryAsync(sql, paramsList, threadNum)(callback)
  }

  /**
   * 执行查询操作
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
  @API
  @deprecated("use query", "fire 2.3.3")
  def executeQuery[T](sql: String, params: Seq[Any] = null, callback: ResultSet => T, keyNum: Int = KeyNum._1): T = {
    JdbcConnector(keyNum = keyNum).executeQuery(sql, params, callback)
  }

  /**
   * 执行查询操作
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
  @API
  def query[T](sql: String, params: Seq[Any] = null, callback: ResultSet => T, keyNum: Int = KeyNum._1): T = {
    JdbcConnector(keyNum = keyNum).query(sql, params, callback)
  }
}
