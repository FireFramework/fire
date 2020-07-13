package com.zto.fire.common.conf

import com.zto.fire.common.util.GlobalConstants.{DefaultVals, PropKeys}
import com.zto.fire.common.util.PropUtils

/**
 * 关系型数据库连接池相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:56
 */
class JdbcConfiguration extends Enumeration {
  // spark.db.jdbc.url
  def url(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_URL_KEY, keyNum)
  // spark.db.jdbc.driver
  def driverClass(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_DRIVER_KEY, keyNum)
  // spark.db.jdbc.user
  def user(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_USER_KEY, keyNum)
  // spark.db.jdbc.password
  def password(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_PASSWORD_KEY, keyNum)
  // 事务的隔离级别：NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, SERIALIZABLE，默认为READ_UNCOMMITTED
  def isolationLevel(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_ISOLATION_LEVEL, keyNum, DefaultVals.jdbcIsolationLevel)
  // 批量操作的记录数
  def batchSize(keyNum: Int = 1): Int = PropUtils.getInt(PropKeys.SPARK_DB_JDBC_BATCH_SIZE, keyNum, DefaultVals.jdbcBatchSize)
  // 默认多少毫秒flush一次
  def jdbcFlushInterval(keyNum: Int = 1): Long = PropUtils.getLong(PropKeys.SPARK_DB_JDBC_FLUSH_INTERVAL, keyNum, 1000)
  // jdbc失败最大重试次数
  def maxRetry(keyNum: Int = 1): Long = PropUtils.getLong(PropKeys.SPARK_DB_JDBC_MAX_RETRY, keyNum, 3)
  // 连接池最小连接数
  def minPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(PropKeys.SPARK_DB_JDBC_MIN_POOL_SIZE_KEY, keyNum, 1)
  // 连接池初始化连接数
  def initialPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(PropKeys.SPARK_DB_JDBC_INITIAL_POOL_SIZE_KEY, keyNum, 1)
  // 连接池最大连接数
  def maxPoolSize(keyNum: Int = 1): Int = PropUtils.getInt(PropKeys.SPARK_DB_JDBC_MAX_POOL_SIZE_KEY, keyNum, 5)
  // 连接池每次自增连接数
  def acquireIncrement(keyNum: Int = 1): Int = PropUtils.getInt(PropKeys.SPARK_DB_JDBC_ACQUIRE_INCREMENT_KEY, keyNum, 1)
  // 多久释放没有用到的连接
  def maxIdleTime(keyNum: Int = 1): Int = PropUtils.getInt(PropKeys.SPARK_DB_JDBC_MAX_IDLE_TIME_KEY, keyNum, 30)
}
