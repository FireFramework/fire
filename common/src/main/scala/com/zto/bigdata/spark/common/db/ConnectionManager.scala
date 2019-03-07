package com.zto.bigdata.spark.common.db

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.zto.bigdata.spark.common.util.GlobalConstants
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
  * 数据库连接池（c3p0）工具类
  * 封装了数据库常用的操作方法
  *
  * @author ChengLong
  *         2016-11-15 16:55:37
  */
class ConnectionManager extends Serializable {
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  try {
    // 从配置文件中读取配置信息，并设置到ComboPooledDataSource对象中
    if(StringUtils.isNotBlank(GlobalConstants.rdburl)) {
      cpds.setJdbcUrl(GlobalConstants.rdburl)
      cpds.setDriverClass(GlobalConstants.driverClass)
      cpds.setUser(GlobalConstants.user)
      cpds.setPassword(GlobalConstants.password)
      cpds.setMaxPoolSize(GlobalConstants.maxPoolSize)
      cpds.setMinPoolSize(GlobalConstants.minPoolSize)
      cpds.setAcquireIncrement(GlobalConstants.acquireIncrement)
      cpds.setInitialPoolSize(GlobalConstants.initialPoolSize)
      cpds.setMaxIdleTime(GlobalConstants.maxIdleTime)
    }
  } catch {
    case ex: Exception => ex.printStackTrace()
  }

  /**
    * 从连接池中获取一个连接
    *
    * @return
    * 数据库连接
    */
  def getConnection(): Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case ex: Exception => ex.printStackTrace()
        null
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

/**
  * 单例的获取连接池
  */
object ConnectionManager {
  var connectionManager: ConnectionManager = _

  /**
    * 获取连接池
    *
    * @return
    */
  def getConnectionManager: ConnectionManager = {
    synchronized {
      if (connectionManager == null) {
        connectionManager = new ConnectionManager
      }
    }
    connectionManager
  }
}