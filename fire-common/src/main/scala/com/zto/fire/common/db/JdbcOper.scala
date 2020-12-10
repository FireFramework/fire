package com.zto.fire.common.db

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.zto.fire.common.anno.Internal
import com.zto.fire.common.conf.{FireFrameworkConf, FireJdbcConf}
import com.zto.fire.common.util.ExceptionBus._
import com.zto.fire.common.util.FireUtils._
import com.zto.fire.common.util.{DBUtils, DataSourceManager, StringsUtils}
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * 数据库连接池（c3p0）工具类
 * 封装了数据库常用的操作方法
 *
 * @param conf
 * 代码级别的配置信息，允许为空，配置文件会覆盖相同配置项，也就是说配置文件拥有着跟高的优先级
 * @param keyNum
 * 用于区分连接不同的数据源，不同配置源对应不同的Oper实例
 * @author ChengLong 2020-11-27 10:31:03
 */
private[fire] class JdbcOper(conf: JdbcConf = null, keyNum: Int = 1) extends DBBaseOper(conf, keyNum) {
  private[this] lazy val connPool = this.init
  // 日志中sql截取的长度
  private lazy val logSqlLength = FireFrameworkConf.logSqlLength
  private[this] var username: String = _
  private[this] var url: String = _
  private[this] var dbType: String = "unknown"
  private[this] lazy val finallyCatchLog = "释放jdbc资源失败"

  /**
   * 初始化指定的连接池，未被使用
   *
   * @return
   * 连接池对象
   */
  @Internal
  private[this] def init: ComboPooledDataSource = {
    tryWithReturn {
      // 从配置文件中读取配置信息，并设置到ComboPooledDataSource对象中
      this.logger.info(s"准备初始化数据库连接池[ ${FireJdbcConf.SPARK_DB_JDBC_URL_KEY}$keyNum ]")
      this.url = if (StringUtils.isBlank(FireJdbcConf.url(keyNum)) && this.conf != null && StringUtils.isNotBlank(this.conf.url)) this.conf.url else FireJdbcConf.url(keyNum)
      require(StringUtils.isNotBlank(this.url), "数据库url不能为空")
      val driverClass = if (StringUtils.isBlank(FireJdbcConf.driverClass(keyNum)) && this.conf != null && StringUtils.isNotBlank(this.conf.driverClass)) this.conf.driverClass else FireJdbcConf.driverClass(keyNum)
      require(StringUtils.isNotBlank(driverClass), "数据库driverClass不能为空")
      this.username = if (StringUtils.isBlank(FireJdbcConf.user(keyNum)) && this.conf != null && StringUtils.isNotBlank(this.conf.username)) this.conf.username else FireJdbcConf.user(keyNum)
      require(StringUtils.isNotBlank(this.username), "数据库username不能为空")
      val password = if (StringUtils.isBlank(FireJdbcConf.password(keyNum)) && this.conf != null && StringUtils.isNotBlank(this.conf.password)) this.conf.password else FireJdbcConf.password(keyNum)
      // 识别数据源类型是oracle、mysql等
      this.dbType = DBUtils.dbTypeParser(driverClass, this.url)
      logger.info(s"架识别到当前jdbc数据源标识为：${this.dbType}")

      // 创建c3p0数据库连接池实例
      val pool = new ComboPooledDataSource(true)
      pool.setJdbcUrl(this.url)
      pool.setDriverClass(driverClass)
      pool.setUser(this.username)
      pool.setPassword(password)
      pool.setMaxPoolSize(FireJdbcConf.maxPoolSize(keyNum))
      pool.setMinPoolSize(FireJdbcConf.minPoolSize(keyNum))
      pool.setAcquireIncrement(FireJdbcConf.acquireIncrement(keyNum))
      pool.setInitialPoolSize(FireJdbcConf.initialPoolSize(keyNum))
      pool.setMaxStatements(0)
      pool.setMaxStatementsPerConnection(0)
      pool.setMaxIdleTime(FireJdbcConf.maxIdleTime(keyNum))
      this.logger.info(s"完成数据库连接池[ $keyNum ] driver: $driverClass")
      pool
    }(this.logger, s"初始化数据库连接池[ $keyNum ]失败")
  }

  /**
   * 从指定的连接池中获取一个连接
   *
   * @return
   * 对应配置项的数据库连接
   */
  def getConnection: Connection = {
    tryWithReturn {
      val connection = this.connPool.getConnection
      this.logger.debug(s"获取数据库连接[ ${keyNum} ]成功")
      connection
    }(this.logger, s"获取数据库连接[ ${FireJdbcConf.SPARK_DB_JDBC_URL_KEY}$keyNum ]发生异常，请检查配置文件")
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
   * @return
   * 影响的记录数
   */
  def executeUpdate(sql: String, params: Seq[Any] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true): Long = {
    val conn = if (connection == null) this.getConnection else connection
    var retVal: Long = 0L
    var stat: PreparedStatement = null
    tryWithFinally {
      val startTime = currentTime
      conn.setAutoCommit(false)
      stat = conn.prepareStatement(sql)

      // 设置值参数
      if (params != null && params.nonEmpty) {
        var i: Int = 1
        params.foreach(param => {
          stat.setObject(i, param)
          i += 1
        })
      }
      retVal = stat.executeUpdate
      if (commit) conn.commit()
      this.logger.info(s"executeUpdate success. keyNum: ${keyNum} count: $retVal cost: ${timecost(startTime)}\n${this.sqlBuriedPoint(sql)}")
      retVal
    } {
      this.release(sql, conn, stat, null, closeConnection)
    }(this.logger, s"executeUpdate failed. keyNum：${keyNum}\n${this.sqlBuriedPoint(sql)}", finallyCatchLog)
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
   * @return
   * 影响的记录数
   */
  def executeBatch(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true): Array[Int] = {
    val conn = if (connection == null) this.getConnection else connection
    var stat: PreparedStatement = null

    tryWithFinally {
      val startTime = currentTime
      conn.setAutoCommit(false)
      stat = conn.prepareStatement(sql)
      var batch = 0
      if (paramsList != null && paramsList.nonEmpty) {
        paramsList.foreach(params => {
          var i = 1
          params.foreach(param => {
            stat.setObject(i, param)
            i += 1
          })
          batch += 1
          stat.addBatch()
          if (batch % FireJdbcConf.batchSize(keyNum) == 0) {
            stat.executeBatch()
            stat.clearBatch()
          }
        })
      }
      // 执行批量更新
      val retVal = stat.executeBatch
      if (commit) conn.commit()
      this.logger.info(s"executeBatch success. keyNum: ${keyNum} count: $batch cost: ${timecost(startTime)}\n${this.sqlBuriedPoint(sql)}")
      retVal
    } {
      this.release(sql, conn, stat, null, closeConnection)
    }(this.logger, s"executeBatch failed. keyNum：${keyNum}\n${this.sqlBuriedPoint(sql)}", finallyCatchLog)
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
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
   */
  def executeQuery[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, clazz: Class[T], connection: Connection = null): List[T] = {
    val listBuffer = ListBuffer[T]()

    this.executeQueryCall(sql, params, new QueryCallback {
      override def process(rs: ResultSet): Int = {
        listBuffer ++= DBUtils.dbResultSet2Bean(rs, clazz)
        listBuffer.size
      }
    }, connection)

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
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
   */
  def executeQueryCall(sql: String, params: Seq[Any] = null, callback: QueryCallback = null, connection: Connection = null): Unit = {
    val conn = if (connection == null) this.getConnection else connection
    var stat: PreparedStatement = null
    var rs: ResultSet = null

    tryWithFinally {
      val startTime = currentTime
      stat = conn.prepareStatement(sql)
      if (params != null && params.nonEmpty) {
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
      this.logger.info(s"executeQueryCall success. keyNum: ${keyNum} count: $count cost: ${timecost(startTime)}\n${this.sqlBuriedPoint(sql, false)}")
    } {
      this.release(sql, conn, stat, rs)
    }(this.logger, s"executeQueryCall failed. keyNum：${keyNum}\n${this.sqlBuriedPoint(sql, false)}", finallyCatchLog)
  }

  /**
   * 释放jdbc资源的工具类
   *
   * @param sql
   * 对应的sql语句
   * @param conn
   * 数据库连接
   * @param rs
   * 查询结果集
   * @param stat
   * jdbc statement
   */
  def release(sql: String, conn: Connection, stat: Statement, rs: ResultSet, closeConnection: Boolean = true): Unit = {
    try {
      if (rs != null) rs.close()
    } catch {
      case e: SQLException => {
        this.logger.error(s"close jdbc ResultSet failed. keyNum: ${keyNum}", e)
        throw e
      }
    } finally {
      try {
        if (stat != null) stat.close()
      } catch {
        case e: SQLException => {
          this.logger.error(s"close jdbc statement failed. keyNum: ${keyNum}", e)
          throw e
        }
      } finally {
        try {
          if (conn != null && closeConnection) conn.close()
        } catch {
          case e: SQLException => {
            this.logger.error(s"close jdbc connection failed. keyNum: ${keyNum}", e)
            throw e
          }
        }
      }
    }
  }

  /**
   * 工具方法，截取给定的SQL语句
   */
  @Internal
  private[this] def sqlBuriedPoint(sql: String, sink: Boolean = true): String = {
    DataSourceManager.addSql(this.dbType, this.url, this.username, sql, sink)
    StringsUtils.substring(sql, 0, this.logSqlLength)
  }

}


/**
 * 内部回调trait，用于处理ResultSet结果集
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

/**
 * jdbc最基本的配置信息，如果配置文件中有，则会覆盖代码中的配置
 *
 * @param url
 * 数据库的url
 * @param driverClass
 * jdbc驱动名称
 * @param username
 * 数据库用户名
 * @param password
 * 数据库密码
 */
case class JdbcConf(url: String, driverClass: String, username: String, password: String)

/**
 * 用于单例构建伴生类JdbcOper的实例对象
 * 每个JdbcOper实例使用keyNum作为标识，并且与每个关系型数据库一一对应
 */
object JdbcOper extends DBBaseOperFactory {

  /**
   * 创建指定集群标识的JdbcOper对象实例
   */
  def apply(conf: JdbcConf = null, keyNum: Int = 1): JdbcOper = {
    if (!this.instanceMap.containsKey(keyNum)) {
      this.instanceMap.put(keyNum, new JdbcOper(conf, keyNum))
    }
    this.instanceMap.get(keyNum).asInstanceOf[JdbcOper]
  }

  // ------------------------------- 兼容老API的使用方法，模拟静态方法的API使用方式 ------------------------------- //

  /**
   * 根据指定的keyNum获取对应的数据库连接
   */
  def getConnection(keyNum: Int = 1): Connection = JdbcOper(keyNum = keyNum).getConnection

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
  def executeUpdate(sql: String, params: Seq[Any] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Long = {
    JdbcOper(keyNum = keyNum).executeUpdate(sql, params, connection, commit, closeConnection)
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
  def executeBatch(sql: String, paramsList: Seq[Seq[Any]] = null, connection: Connection = null, commit: Boolean = true, closeConnection: Boolean = true, keyNum: Int = 1): Array[Int] = {
    JdbcOper(keyNum = keyNum).executeBatch(sql, paramsList, connection, commit, closeConnection)
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
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   */
  def executeQuery[T <: Object : ClassTag](sql: String, params: Seq[Any] = null, clazz: Class[T], connection: Connection = null, keyNum: Int = 1): List[T] = {
    JdbcOper(keyNum = keyNum).executeQuery(sql, params, clazz, connection)
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
   * 传递已有的数据库连接，可满足跨api的同一事务提交的需求
   * @param keyNum
   * 配置文件中数据源配置的数字后缀，用于应对多数据源的情况，如果仅一个数据源，可不填
   * 比如需要操作另一个数据库，那么配置文件中key需携带相应的数字后缀：spark.db.jdbc.url2，那么此处方法调用传参为3，以此类推
   */
  def executeQueryCall(sql: String, params: Seq[Any] = null, callback: QueryCallback = null, connection: Connection = null, keyNum: Int = 1): Unit = {
    JdbcOper(keyNum = keyNum).executeQueryCall(sql, params, callback, connection)
  }
}