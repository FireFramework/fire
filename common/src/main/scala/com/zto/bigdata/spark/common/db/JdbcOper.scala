package com.zto.bigdata.spark.common.db

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.zto.bigdata.spark.common.ext.SparkExt._
import com.zto.bigdata.spark.common.util.{GlobalConstants, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.Logging

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * 数据库连接池（c3p0）工具类
  * 封装了数据库常用的操作方法
  *
  * @author ChengLong 2016-11-15 16:55:37
  */
object JdbcOper extends Logging with Serializable {
  private lazy val connPoolMap = collection.mutable.Map[String, ComboPooledDataSource]()
  private val jdbcPoolKey = "cpds"

  try {
    (1 to 10).foreach(i => {
      // 从配置文件中读取配置信息，并设置到ComboPooledDataSource对象中
      if (StringUtils.isNotBlank(GlobalConstants.JdbcConf.url(i)) && StringUtils.isNotBlank(GlobalConstants.JdbcConf.user(i))) {
        this.log("jdbc", "init", s"初始化数据库连接池[ ${GlobalConstants.PropKeys.SPARK_DB_JDBC_URL_KEY}$i ]", null, true)
        val cpds = new ComboPooledDataSource(true)
        cpds.setJdbcUrl(GlobalConstants.JdbcConf.url(i))
        cpds.setDriverClass(GlobalConstants.JdbcConf.driverClass(i))
        cpds.setUser(GlobalConstants.JdbcConf.user(i))
        cpds.setPassword(GlobalConstants.JdbcConf.password(i))
        cpds.setMaxPoolSize(GlobalConstants.JdbcConf.maxPoolSize(i))
        cpds.setMinPoolSize(GlobalConstants.JdbcConf.minPoolSize(i))
        cpds.setAcquireIncrement(GlobalConstants.JdbcConf.acquireIncrement(i))
        cpds.setInitialPoolSize(GlobalConstants.JdbcConf.initialPoolSize(i))
        cpds.setMaxIdleTime(GlobalConstants.JdbcConf.maxIdleTime(i))
        this.connPoolMap += (s"cpds$i" -> cpds)
        this.log("jdbc", "init", s"数据库连接池[ ${GlobalConstants.PropKeys.SPARK_DB_JDBC_URL_KEY}$i ]初始化成功：url: ${GlobalConstants.JdbcConf.url(i)} driver: ${GlobalConstants.JdbcConf.driverClass(i)} ", null, true)
      }
    })
  } catch {
    case ex: Exception => ex.printStackTrace()
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
    try {
      val pool = this.connPoolMap.get(s"${this.jdbcPoolKey}$keyNum")
      return pool.get.getConnection
    } catch {
      case ex: Exception => this.log("jdbc", s"getConnection(${keyNum})", s"获取数据库连接[ ${GlobalConstants.PropKeys.SPARK_DB_JDBC_URL_KEY}$keyNum ]出现异常，请检查配置文件", ex, true)
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
    * @param keyNum
    * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
    * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
    * @return
    * 影响的记录数
    */
  def executeUpdate(sql: String, params: Seq[Any], connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Long = {
    this.mark
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
      this.log("jdbc", "update", s"sql->$sql 影响记录数：$retVal", null, true)
    }
    catch {
      case e: Exception => this.log("jdbc", "update", s"sql->$sql result->fail", e, true)
    } finally {
      if (conn != null && closeConnection)
        conn.close()
      if (stat != null) {
        try {
          stat.close()
        } catch {
          case e: SQLException => this.log("jdbc", "释放连接", s"sql->$sql", e, true)
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
  def executeBatch(sql: String, paramsList: Seq[Seq[Any]], connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Array[Int] = {
    this.mark
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
            this.log("jdbc", s"executeBatch-->batch=${GlobalConstants.JdbcConf.batchSize(keyNum)}", s"sql->$sql 影响记录数：${GlobalConstants.JdbcConf.batchSize(keyNum)}", null, true)
          }
        })
      }
      // 执行批量更新
      retVal = stat.executeBatch
      if (commit) conn.commit()
      this.log("jdbc", "executeBatch", s"sql->$sql 影响总记录数：$batch", null, true)
    } catch {
      case e: Exception => this.log("jdbc", "executeBatch", s"sql->$sql result->fail", e, true)
    } finally {
      if (conn != null && closeConnection) conn.close()
      if (stat != null) {
        try {
          stat.close()
        } catch {
          case e: SQLException => this.log("jdbc", "释放连接", sql, e, true)
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
  def executeQuery[T <: Object : ClassTag](sql: String, params: Seq[Any], clazz: Class[T], connection: Connection = null, keyNum: Int = 1): List[T] = {
    val listBuffer = ListBuffer[T]()

    this.executeQueryCall(sql, params, new QueryCallback {
      override def process(rs: ResultSet): Int = {
        listBuffer ++= SparkUtils.dbResultSet2Bean(rs, clazz)
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
  def executeQueryCall(sql: String, params: Seq[Any], callback: QueryCallback, connection: Connection = null, keyNum: Int = 1): Unit = {
    this.mark
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
      if (rs != null) {
        count = callback.process(rs)
      }
      this.log("jdbc", "query", s"sql->$sql result->success 查询记录数：$count", null, true)
    } catch {
      case e: Exception => this.log("jdbc", "query", s"sql->$sql result->fail", e, true)
    } finally {
      if (conn != null) conn.close()
      if (rs != null) {
        try {
          rs.close()
        } catch {
          case e: SQLException => this.log("db", "释放连接", s"sql->$sql", e, true)
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