package com.zto.fire.common.db

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.zto.fire.common.bean.BaseLogging
import com.zto.fire.common.util.{DBUtils, GlobalConstants, StringsUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.LogManager

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * 数据库连接池（c3p0）工具类
  * 封装了数据库常用的操作方法
  *
  * @author ChengLong 2016-11-15 16:55:37
  */
object JdbcOper extends BaseLogging {
  private lazy val connPoolMap = collection.mutable.Map[String, ComboPooledDataSource]()
  private val logger = LogManager.getLogger(this.getClass)
  // 日志中sql截取的长度
  private lazy val logSqlLength = GlobalConstants.FireConf.logSqlLength
  private val jdbcPoolKey = "cpds"

  /**
    * 初始化指定的连接池，未被使用
    *
    * @param keyNum
    * 连接池配置数字后缀
    * @return
    * 连接池
    */
  def init(keyNum: Int = 1): ComboPooledDataSource = {
    var pool = this.connPoolMap.get(s"${this.jdbcPoolKey}$keyNum").getOrElse(null)
    if (pool == null) {
      try {
        // 从配置文件中读取配置信息，并设置到ComboPooledDataSource对象中
        if (StringUtils.isNotBlank(GlobalConstants.JdbcConf.url(keyNum)) && StringUtils.isNotBlank(GlobalConstants.JdbcConf.user(keyNum))) {
          this.logger.info(s"准备初始化数据库连接池[ ${GlobalConstants.PropKeys.SPARK_DB_JDBC_URL_KEY}$keyNum ]")
          pool = new ComboPooledDataSource(true)
          pool.setJdbcUrl(GlobalConstants.JdbcConf.url(keyNum))
          pool.setDriverClass(GlobalConstants.JdbcConf.driverClass(keyNum))
          pool.setUser(GlobalConstants.JdbcConf.user(keyNum))
          pool.setPassword(GlobalConstants.JdbcConf.password(keyNum))
          pool.setMaxPoolSize(GlobalConstants.JdbcConf.maxPoolSize(keyNum))
          pool.setMinPoolSize(GlobalConstants.JdbcConf.minPoolSize(keyNum))
          pool.setAcquireIncrement(GlobalConstants.JdbcConf.acquireIncrement(keyNum))
          pool.setInitialPoolSize(GlobalConstants.JdbcConf.initialPoolSize(keyNum))
          pool.setMaxStatements(0)
          pool.setMaxStatementsPerConnection(0)
          pool.setMaxIdleTime(GlobalConstants.JdbcConf.maxIdleTime(keyNum))
          this.connPoolMap += (s"cpds$keyNum" -> pool)
          this.logger.info(s"完成数据库连接池[ ${GlobalConstants.PropKeys.SPARK_DB_JDBC_URL_KEY}$keyNum ] 初始化：url: ${GlobalConstants.JdbcConf.url(keyNum)} driver: ${GlobalConstants.JdbcConf.driverClass(keyNum)} ")
        }
      } catch {
        case ex: Exception => {
          this.logger.error(s"初始化数据库连接池[ ${GlobalConstants.PropKeys.SPARK_DB_JDBC_URL_KEY}$keyNum ]失败", ex)
          throw ex
        }
      }
    }
    pool
  }

  /**
    * 从指定的连接池中获取一个连接
    *
    * @param keyNum
    * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
    * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
    * @return
    * 对应配置项的数据库连接
    */
  def getConnection(keyNum: Int = 1): Connection = {
    var connection: Connection = null
    try {
      val pool = this.init(keyNum)
      connection = pool.getConnection
      this.logger.info(s"getConnection(${keyNum}) 获取数据库连接[ ${keyNum} ]成功")
    } catch {
      case ex: Exception => {
        this.logger.error(s"getConnection(${keyNum}) 获取数据库连接[ ${GlobalConstants.PropKeys.SPARK_DB_JDBC_URL_KEY}$keyNum ]出现异常，请检查配置文件", ex)
        throw ex
      }
    }
    connection
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
    * @param keyNum
    * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
    * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
    * @return
    * 影响的记录数
    */
  def executeUpdate(sql: String, params: Seq[Any] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Long = {
    var retVal: Long = 0L
    var conn: Connection = connection
    var stat: PreparedStatement = null
    try {
      if (conn == null) {
        conn = this.getConnection(keyNum)
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
      this.logger.info(s"executeUpdate: sql->${StringsUtils.substring(sql, 0, this.logSqlLength)} 影响记录数：$retVal")
    }
    catch {
      case e: Exception => {
        this.logger.error(s"executeUpdate: sql->${StringsUtils.substring(sql, 0, this.logSqlLength)} result->fail", e)
        throw e
      }
    } finally {
      if (conn != null && closeConnection)
        conn.close()
      if (stat != null) {
        try {
          stat.close()
        } catch {
          case e: SQLException => {
            this.logger.error(s"executeUpdate: 释放连接 sql->${StringsUtils.substring(sql, 0, this.logSqlLength)}", e)
            throw e
          }
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
    * @param keyNum
    * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
    * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
    * @return
    * 影响的记录数
    */
  def executeBatch(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Array[Int] = {
    var retVal: Array[Int] = null
    var conn: Connection = connection
    var stat: PreparedStatement = null
    try {
      if (conn == null) {
        conn = this.getConnection(keyNum)
        conn.setAutoCommit(false)
      }
      stat = conn.prepareStatement(sql)
      var batch = 0
      if (paramsList != null && paramsList.size > 0) {
        paramsList.foreach(params => {
          var i = 1
          params.foreach(param => {
            stat.setObject(i, param)
            i += 1
          })
          batch += 1
          stat.addBatch()
          if (batch % GlobalConstants.JdbcConf.batchSize(keyNum) == 0) {
            stat.executeBatch()
            stat.clearBatch()
          }
        })
      }
      // 执行批量更新
      retVal = stat.executeBatch
      if (commit) conn.commit()
      this.logger.info(s"executeBatch: sql->${StringsUtils.substring(sql, 0, this.logSqlLength)} 影响总记录数：$batch")
    } catch {
      case e: Exception => {
        this.logger.error(s"executeBatch sql->${StringsUtils.substring(sql, 0, this.logSqlLength)} result->fail", e)
        throw e
      }
    } finally {
      if (conn != null && closeConnection) conn.close()
      if (stat != null) {
        try {
          stat.close()
        } catch {
          case e: SQLException => {
            this.logger.error(s"executeBatch: 释放连接 sql->${StringsUtils.substring(sql, 0, this.logSqlLength)}", e)
            throw e
          }
        }
      }
    }
    retVal
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
    * @param connection
    * 使用制定的数据库连接
    * @param keyNum
    * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
    * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
    */
  def executeQuery[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, clazz: Class[T], connection: Connection = null, keyNum: Int = 1): List[T] = {
    val listBuffer = ListBuffer[T]()

    this.executeQueryCall(sql, params, new QueryCallback {
      override def process(rs: ResultSet): Int = {
        listBuffer ++= DBUtils.dbResultSet2Bean(rs, clazz)
        listBuffer.size
      }
    }, connection, keyNum)

    listBuffer.toList
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
    * @param connection
    * 使用制定的数据库连接
    * @param keyNum
    * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
    * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
    */
  def executeQueryCall(sql: String, params: Seq[Any] = null, callback: QueryCallback = null, connection: Connection = null, keyNum: Int = 1): Unit = {
    var conn: Connection = connection
    var stat: PreparedStatement = null
    var rs: ResultSet = null
    try {
      if (conn == null) {
        conn = this.getConnection(keyNum)
      }
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
      if (rs != null && callback != null) {
        count = callback.process(rs)
      }
      this.logger.info(s"executeQueryCall: sql->${StringsUtils.substring(sql, 0, this.logSqlLength)} result->success 查询记录数：$count")
    } catch {
      case e: Exception => {
        this.logger.error(s"executeQueryCall: sql->${StringsUtils.substring(sql, 0, this.logSqlLength)} result->fail", e)
        throw e
      }
    } finally {
      if (conn != null) conn.close()
      if (rs != null) {
        try {
          rs.close()
        } catch {
          case e: SQLException => {
            this.logger.error(s"executeQueryCall: 释放连接 sql->${StringsUtils.substring(sql, 0, this.logSqlLength)}", e)
            throw e
          }
        }
      }
      if (stat != null) {
        try {
          stat.close()
        }
        catch {
          case e: SQLException => {
            this.logger.error(s"executeQueryCall: 释放连接 sql->${StringsUtils.substring(sql, 0, this.logSqlLength)}", e)
            throw e
          }
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
trait QueryCallback extends BaseLogging {

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