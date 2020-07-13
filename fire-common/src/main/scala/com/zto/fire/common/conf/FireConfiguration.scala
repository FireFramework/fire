package com.zto.fire.common.conf

import com.zto.fire.common.util.GlobalConstants.{DefaultVals, PropKeys}
import com.zto.fire.common.util.{GlobalConstants, PropUtils}

/**
 * Fire框架相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:54
 */
class FireConfiguration extends Enumeration {
  // rest接口权限认证
  lazy val restFilter = PropUtils.getBoolean(GlobalConstants.PropKeys.SPARK_FIRE_REST_FILTER_ENABLE, GlobalConstants.DefaultVals.restFilter)
  // 是否关闭fire内置的所有累加器
  lazy val accEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_ACC_ENABLE, true)
  // 日志累加器开关
  lazy val accLogEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_ACC_LOG_ENABLE, true)
  // 多值累加器开关
  lazy val accMultiCounterEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_ACC_MULTI_COUNTER_ENABLE, true)
  // 多时间维度累加器开关
  lazy val accMultiTimerEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_ACC_MULTI_TIMER_ENABLE, true)
  // fire框架埋点日志开关
  lazy val logEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_LOG_ENABLE, true)
  // 用于限定fire框架中sql日志的字符串长度
  lazy val logSqlLength = PropUtils.getInt(PropKeys.SPARK_FIRE_LOG_SQL_LENGTH, DefaultVals.logSqlLength)
  // HBase结果集的缓存策略配置
  lazy val hbaseStorageLevelConf = PropUtils.getString(PropKeys.SPARK_FIRE_HBASE_STORAGE_LEVEL, "memory_and_disk_ser").toUpperCase
  // 通过HBase scan后repartition的分区数，默认1200
  lazy val hbaseHadoopScanRepartitions = PropUtils.getInt(PropKeys.SPARK_FIRE_HBASE_SCAN_REPARTITIONS, 1200)
  // fire框架针对jdbc操作后数据集的缓存策略
  lazy val jdbcStorageLevelConf = PropUtils.getString(PropKeys.SPARK_FIRE_JDBC_STORAGE_LEVEL, "memory_and_disk_ser").toUpperCase
  // 通过JdbcOper查询后将数据集放到多少个分区中，需根据实际的结果集做配置
  lazy val jdbcQueryPartitions = PropUtils.getInt(PropKeys.SPARK_FIRE_JDBC_QUERY_REPARTITIONS, 10)
  // fire框架rest接口服务最大线程数
  lazy val restfulMaxThread = PropUtils.getInt(PropKeys.SPARK_FIRE_RESTFUL_MAX_THREAD, 8)
  // 用于配置是否抛弃zrc独立运行，配置为false表示不向zrc注册，不获取zrc配置
  lazy val zrcEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_ZRC_ENABLE, true)
  // zrc接口调用秘钥
  lazy val zrcSecret = PropUtils.getString(PropKeys.SPARK_FIRE_ZRC_SECRET, "21fa30b7f2082b1b12dfbc7c8c6d70b9")
  // fire框架restful端口冲突重试次数
  lazy val restfulPortRetryNum = PropUtils.getInt(PropKeys.SPARK_FIRE_RESTFUL_PORT_RETRY_NUM, 3)
  // fire框架restful端口冲突重试时间（ms）
  lazy val restfulPortRetryDuration = PropUtils.getLong(PropKeys.SPARK_FIRE_RESTFUL_PORT_RETRY_DURATION, 1000L)
  // 获取配置的HBase缓存策略
  def hbaseStorageLevel: String = hbaseStorageLevelConf
  // 获取配置的JDBC缓存策略
  def jdbcStorageLevel: String = jdbcStorageLevelConf
}
