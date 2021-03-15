package com.zto.fire.jdbc.conf

import com.zto.fire.common.util.PropUtils

/**
 * 关系型数据库连接池相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:56
 */
private[fire] object FireJdbcConf {
  // c3p0连接池相关配置
  lazy val JDBC_URL = "db.jdbc.url"
  lazy val JDBC_DRIVER = "db.jdbc.driver"
  lazy val JDBC_USER = "db.jdbc.user"
  lazy val JDBC_PASSWORD = "db.jdbc.password"
  lazy val JDBC_ISOLATION_LEVEL = "db.jdbc.isolation.level"
  lazy val JDBC_MAX_POOL_SIZE = "db.jdbc.maxPoolSize"
  lazy val JDBC_MIN_POOL_SIZE = "db.jdbc.minPoolSize"
  lazy val JDBC_ACQUIRE_INCREMENT = "db.jdbc.acquireIncrement"
  lazy val JDBC_INITIAL_POOL_SIZE = "db.jdbc.initialPoolSize"
  lazy val JDBC_MAX_IDLE_TIME = "db.jdbc.maxIdleTime"
  lazy val JDBC_BATCH_SIZE = "db.jdbc.batch.size"
  lazy val JDBC_FLUSH_INTERVAL = "db.jdbc.flushInterval"
  lazy val JDBC_MAX_RETRY = "db.jdbc.max.retry"
  // fire框架针对jdbc操作后数据集的缓存策略
  lazy val FIRE_JDBC_STORAGE_LEVEL = "fire.jdbc.storage.level"
  // 通过JdbcConnector查询后将数据集放到多少个分区中，需根据实际的结果集做配置
  lazy val FIRE_JDBC_QUERY_REPARTITION = "fire.jdbc.query.partitions"

  // 默认的事务隔离级别
  lazy val jdbcIsolationLevel = "READ_UNCOMMITTED"
  // 数据库批量操作的记录数
  lazy val jdbcBatchSize = 1000
  // fire框架针对jdbc操作后数据集的缓存策略
  lazy val jdbcStorageLevel = PropUtils.getString(this.FIRE_JDBC_STORAGE_LEVEL, "memory_and_disk_ser").toUpperCase
  // 通过JdbcConnector查询后将数据集放到多少个分区中，需根据实际的结果集做配置
  lazy val jdbcQueryPartition = PropUtils.getInt(this.FIRE_JDBC_QUERY_REPARTITION, 10)

  // db.jdbc.url
  def url(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_URL, keyNum)
  // db.jdbc.driver
  def driverClass(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_DRIVER, keyNum)
  // db.jdbc.user
  def user(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_USER, keyNum)
  // db.jdbc.password
  def password(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_PASSWORD, keyNum)
  // 事务的隔离级别：NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, SERIALIZABLE，默认为READ_UNCOMMITTED
  def isolationLevel(keyNum: Int = 1): String = PropUtils.getString(this.JDBC_ISOLATION_LEVEL, keyNum, this.jdbcIsolationLevel)
  // 批量操作的记录数
  def batchSize(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_BATCH_SIZE, keyNum, this.jdbcBatchSize)
  // 默认多少毫秒flush一次
  def jdbcFlushInterval(keyNum: Int = 1): Long = PropUtils.getLong(this.JDBC_FLUSH_INTERVAL, keyNum, 1000)
  // jdbc失败最大重试次数
  def maxRetry(keyNum: Int = 1): Long = PropUtils.getLong(this.JDBC_MAX_RETRY, keyNum, 3)
  // 连接池最小连接数
  def minPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_MIN_POOL_SIZE, keyNum, 1)
  // 连接池初始化连接数
  def initialPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_INITIAL_POOL_SIZE, keyNum, 1)
  // 连接池最大连接数
  def maxPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_MAX_POOL_SIZE, keyNum, 5)
  // 连接池每次自增连接数
  def acquireIncrement(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_ACQUIRE_INCREMENT, keyNum, 1)
  // 多久释放没有用到的连接
  def maxIdleTime(keyNum: Int = 1): Int = PropUtils.getInt(this.JDBC_MAX_IDLE_TIME, keyNum, 30)
}
