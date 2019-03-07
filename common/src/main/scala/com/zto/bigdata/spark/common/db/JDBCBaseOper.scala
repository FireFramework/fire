package com.zto.bigdata.spark.common.db

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import scala.collection.mutable.ListBuffer

/**
  * JDBC公共API
  * Created by ChengLong on 2017-10-20.
  */
trait JDBCBaseOper extends Serializable {

  /**
    * 从连接池中获取一个连接
    *
    * @return
    * 数据库连接
    */
  def getConnection(): Connection

  /**
    * 更新操作
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
    * @return
    * 影响的记录数
    */
  def executeUpdate(sql: String, params: Array[Any], connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true): Long = {
    var retVal: Long = 0L
    var conn: Connection = connection
    var stat: PreparedStatement = null
    try {
      if (conn == null) {
        conn = this.getConnection
        conn.setAutoCommit(false)
      }
      stat = conn.prepareStatement(sql)

      // 设置值参数
      if (params != null && params.length > 0) {
        var i: Int = 1
        params.foreach(param => {
          stat.setObject(i, param)
          i += 1
        })
      }
      retVal = stat.executeUpdate
      if (commit) conn.commit()
    }
    catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null && closeConnection)
        conn.close()
      if (stat != null) {
        try {
          stat.close()
        } catch {
          case e: SQLException => e.printStackTrace()
        }
      }
    }
    retVal
  }

  /**
    * 执行批量更新操作
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
    * @return
    * 影响的记录数
    */
  def executeBatch(sql: String, paramsList: ListBuffer[Array[Any]], connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true): Array[Int] = {
    var retVal: Array[Int] = null
    var conn: Connection = connection
    var stat: PreparedStatement = null
    try {
      if (conn == null) {
        conn = this.getConnection
        conn.setAutoCommit(false)
      }
      stat = conn.prepareStatement(sql)
      if (paramsList != null && paramsList.size > 0) {
        paramsList.foreach(params => {
          var i = 1
          params.foreach(param => {
            stat.setObject(i, param)
            i += 1
          })
          stat.addBatch()
        })
      }
      // 执行批量更新
      retVal = stat.executeBatch
      if (commit) conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null && closeConnection) conn.close()
      if (stat != null) {
        try {
          stat.close()
        } catch {
          case e: SQLException => e.printStackTrace()
        }
      }
    }
    retVal
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
    */
  def executeQuery(sql: String, params: Array[Any], callback: QueryCallback) {
    var conn: Connection = null
    var stat: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = this.getConnection
      stat = conn.prepareStatement(sql)
      if (params != null && params.length > 0) {
        var i = 1
        params.foreach(param => {
          stat.setObject(i, param)
          i += 1
        })
      }
      rs = stat.executeQuery
      callback.process(rs)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (conn != null) conn.close()
      if (rs != null) {
        try {
          rs.close()
        } catch {
          case e: SQLException => e.printStackTrace()
        }
      }
      if (stat != null) {
        try {
          stat.close()
        }
        catch {
          case e: SQLException => e.printStackTrace()
        }
      }
    }
  }

  /**
    * 内部回调trait
    *
    * @author ChengLong
    *         2016-11-16 09:22:11
    */
  trait QueryCallback {
    // 回调方法，对返回结果进行处理
    @throws[Exception]
    def process(rs: ResultSet)
  }
}
