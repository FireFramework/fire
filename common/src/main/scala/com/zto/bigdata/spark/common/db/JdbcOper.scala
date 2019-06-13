package com.zto.bigdata.spark.common.db

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.{GlobalConstants, ParamUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * 数据库连接池（c3p0）工具类
  * 封装了数据库常用的操作方法
  *
  * @author ChengLong 2016-11-15 16:55:37
  */
object JdbcOper extends Serializable {
  private lazy val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  try {
    // 从配置文件中读取配置信息，并设置到ComboPooledDataSource对象中
    if (ParamUtils.isNotBlank(GlobalConstants.JdbcConf.url, GlobalConstants.JdbcConf.user)) {
      this.logger.wrapLogInfo("初始化数据库连接池")
      cpds.setJdbcUrl(GlobalConstants.JdbcConf.url)
      cpds.setDriverClass(GlobalConstants.JdbcConf.driverClass)
      cpds.setUser(GlobalConstants.JdbcConf.user)
      cpds.setPassword(GlobalConstants.JdbcConf.password)
      cpds.setMaxPoolSize(GlobalConstants.JdbcConf.maxPoolSize)
      cpds.setMinPoolSize(GlobalConstants.JdbcConf.minPoolSize)
      cpds.setAcquireIncrement(GlobalConstants.JdbcConf.acquireIncrement)
      cpds.setInitialPoolSize(GlobalConstants.JdbcConf.initialPoolSize)
      cpds.setMaxIdleTime(GlobalConstants.JdbcConf.maxIdleTime)
      this.logger.wrapLogInfo(s"数据库连接池初始化成功：url: ${GlobalConstants.JdbcConf.url} driver: ${GlobalConstants.JdbcConf.driverClass} ")
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
      return cpds.getConnection()
    } catch {
      case ex: Exception => this.logger.error("获取数据库连接出现异常", ex)
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
    this.logger.mark
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
      this.logger.log("db", "update", s"sql->$sql 影响记录数：$retVal")
    }
    catch {
      case e: Exception => this.logger.log("db", "update", s"sql->$sql result->fail", e)
    } finally {
      if (conn != null && closeConnection)
        conn.close()
      if (stat != null) {
        try {
          stat.close()
        } catch {
          case e: SQLException => this.logger.log("db", "释放连接", s"sql->$sql", e)
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
    this.logger.mark
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
      this.logger.log("db", "executeBatch", s"sql->$sql 影响记录数：$retVal")
    } catch {
      case e: Exception => this.logger.log("db", "executeBatch", s"sql->$sql result->fail", e)
    } finally {
      if (conn != null && closeConnection) conn.close()
      if (stat != null) {
        try {
          stat.close()
        } catch {
          case e: SQLException => this.logger.log("db", "释放连接", sql, e)
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
  def executeQuery(sql: String, params: Array[Any], callback: QueryCallback): Unit = {
    this.logger.mark
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
      var count: Long = 0
      if (rs != null) {
        count = callback.process(rs)
      }
      this.logger.log("db", "query", s"sql->$sql result->success 查询记录数：$count")
    } catch {
      case e: Exception => this.logger.log("db", "query", s"sql->$sql result->fail", e)
    } finally {
      if (conn != null) conn.close()
      if (rs != null) {
        try {
          rs.close()
        } catch {
          case e: SQLException => this.logger.log("db", "释放连接", s"sql->$sql", e)
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

  /**
    * 回调方法，对返回结果进行处理
    *
    * @param rs
    * 查询的结果集
    * @return
    * 结果集记录数
    */
  @throws[Exception]
  def process(rs: ResultSet): Int
}