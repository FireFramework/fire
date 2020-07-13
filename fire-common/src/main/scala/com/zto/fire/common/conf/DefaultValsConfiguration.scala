package com.zto.fire.common.conf

/**
 * 预定义的默认值，配置文件没有指明的情况下会取默认值
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:52
 */
class DefaultValsConfextends extends Enumeration {
  // hbase集群名称标识
  val hbaseName = "batch"
  // rest接口filter的开关
  val restFilter = true
  // 数据丢失时执行失败
  val kafkaFailOnDataLoss = true
  // enable.auto.commit
  val kafkaEnableAutoCommit = false
  // 默认的事务隔离级别
  val jdbcIsolationLevel = "READ_UNCOMMITTED"
  // 数据库批量操作的记录数
  val jdbcBatchSize = 1000
  // 数据丢失时执行失败
  val rocketFailOnDataLoss = true
  // enable.auto.commit
  val rocketEnableAutoCommit = false
  // 订阅的tag
  val rocketConsumerTag = "*"
  // spark 默认的checkpoint地址
  val sparkChkPointDir = "hdfs://nameservice1/user/spark/ckpoint/"
  // 默认的日志级别
  val logLevel = "info"
  // 累加器保留日志默认的最少记录数
  val minLogSize = 500
  // 累加器保留日志默认的最大记录数
  val maxLogSize = 1000
  // env累加器保留的最大记录数
  val maxEnvSize = 500
  // env累加器保留的最少记录数
  val minEnvSize = 100
  val maxTimerSize = 1000
  val maxTimerHour = 12
  // 默认的数据库名称
  val dbName = "tmp"
  // 默认的partition名称
  val partitionName = "ds"
  // HBase默认批次大小
  val hbaseBatch = 10000
  // 启用高可用
  val enableHdfsHA = true
  // fire框架中sql日志的默认打印长度
  val logSqlLength = 50

}
