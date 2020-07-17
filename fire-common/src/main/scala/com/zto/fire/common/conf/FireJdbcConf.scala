package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils

/**
 * 关系型数据库连接池相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:56
 */
private[fire] object FireJdbcConf {
  // c3p0连接池相关配置
  lazy val SPARK_DB_JDBC_URL_KEY = "spark.db.jdbc.url"
  lazy val SPARK_DB_JDBC_DRIVER_KEY = "spark.db.jdbc.driver"
  lazy val SPARK_DB_JDBC_USER_KEY = "spark.db.jdbc.user"
  lazy val SPARK_DB_JDBC_PASSWORD_KEY = "spark.db.jdbc.password"
  lazy val SPARK_DB_JDBC_ISOLATION_LEVEL = "spark.db.jdbc.isolation.level"
  lazy val SPARK_DB_JDBC_MAX_POOL_SIZE_KEY = "spark.db.jdbc.maxPoolSize"
  lazy val SPARK_DB_JDBC_MIN_POOL_SIZE_KEY = "spark.db.jdbc.minPoolSize"
  lazy val SPARK_DB_JDBC_ACQUIRE_INCREMENT_KEY = "spark.db.jdbc.acquireIncrement"
  lazy val SPARK_DB_JDBC_INITIAL_POOL_SIZE_KEY = "spark.db.jdbc.initialPoolSize"
  lazy val SPARK_DB_JDBC_MAX_IDLE_TIME_KEY = "spark.db.jdbc.maxIdleTime"
  lazy val SPARK_DB_JDBC_BATCH_SIZE = "spark.db.jdbc.batch.size"
  lazy val SPARK_DB_JDBC_FLUSH_INTERVAL = "spark.db.jdbc.flushInterval"
  lazy val SPARK_DB_JDBC_MAX_RETRY = "spark.db.jdbc.max.retry"
  // fire框架针对jdbc操作后数据集的缓存策略
  lazy val SPARK_FIRE_JDBC_STORAGE_LEVEL = "spark.fire.jdbc.storage.level"
  // 通过JdbcOper查询后将数据集放到多少个分区中，需根据实际的结果集做配置
  lazy val SPARK_FIRE_JDBC_QUERY_REPARTITIONS = "spark.fire.jdbc.query.partitions"

  // 默认的事务隔离级别
  lazy val jdbcIsolationLevel = "READ_UNCOMMITTED"
  // 数据库批量操作的记录数
  lazy val jdbcBatchSize = 1000
  // fire框架针对jdbc操作后数据集的缓存策略
  lazy val jdbcStorageLevel = PropUtils.getString(this.SPARK_FIRE_JDBC_STORAGE_LEVEL, "memory_and_disk_ser").toUpperCase
  // 通过JdbcOper查询后将数据集放到多少个分区中，需根据实际的结果集做配置
  lazy val jdbcQueryPartitions = PropUtils.getInt(this.SPARK_FIRE_JDBC_QUERY_REPARTITIONS, 10)

  // spark.db.jdbc.url
  def url(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DB_JDBC_URL_KEY, keyNum)
  // spark.db.jdbc.driver
  def driverClass(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DB_JDBC_DRIVER_KEY, keyNum)
  // spark.db.jdbc.user
  def user(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DB_JDBC_USER_KEY, keyNum)
  // spark.db.jdbc.password
  def password(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DB_JDBC_PASSWORD_KEY, keyNum)
  // 事务的隔离级别：NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, SERIALIZABLE，默认为READ_UNCOMMITTED
  def isolationLevel(keyNum: Int = 1): String = PropUtils.getString(this.SPARK_DB_JDBC_ISOLATION_LEVEL, keyNum, this.jdbcIsolationLevel)
  // 批量操作的记录数
  def batchSize(keyNum: Int = 1): Int = PropUtils.getInt(this.SPARK_DB_JDBC_BATCH_SIZE, keyNum, this.jdbcBatchSize)
  // 默认多少毫秒flush一次
  def jdbcFlushInterval(keyNum: Int = 1): Long = PropUtils.getLong(this.SPARK_DB_JDBC_FLUSH_INTERVAL, keyNum, 1000)
  // jdbc失败最大重试次数
  def maxRetry(keyNum: Int = 1): Long = PropUtils.getLong(this.SPARK_DB_JDBC_MAX_RETRY, keyNum, 3)
  // 连接池最小连接数
  def minPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.SPARK_DB_JDBC_MIN_POOL_SIZE_KEY, keyNum, 1)
  // 连接池初始化连接数
  def initialPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.SPARK_DB_JDBC_INITIAL_POOL_SIZE_KEY, keyNum, 1)
  // 连接池最大连接数
  def maxPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(this.SPARK_DB_JDBC_MAX_POOL_SIZE_KEY, keyNum, 5)
  // 连接池每次自增连接数
  def acquireIncrement(keyNum: Int = 1): Int = PropUtils.getInt(this.SPARK_DB_JDBC_ACQUIRE_INCREMENT_KEY, keyNum, 1)
  // 多久释放没有用到的连接
  def maxIdleTime(keyNum: Int = 1): Int = PropUtils.getInt(this.SPARK_DB_JDBC_MAX_IDLE_TIME_KEY, keyNum, 30)
}