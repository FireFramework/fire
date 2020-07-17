package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils

/**
 * 常量配置类
 * Created by ChengLong on 2016-11-22.
 */
private[fire] class FireConf {
  // 用于区分不同的流计算引擎类型
  private[fire] lazy val engine = PropUtils.keyPrefix

  // Fire框架相关配置
  val frameworkConf = FireFrameworkConf
  // flink相关配置
  val flinkConf = FireFlinkConf
  // 关系型数据库连接池相关配置
  val jdbcConf = FireJdbcConf
  // Spark相关常量配置
  val sparkConf = FireSparkConf
  // kafka相关配置
  val kafkaConf = FireKafkaConf
  // rocketMQ相关配置
  val rocketConf = FireRocketConf
  // hbase相关配置
  val hbaseConf = FireHBaseConf
  // impala相关配置
  val kuduConf = FireKuduConf
  // 周期相关字符串
  val cronConf = FireCronConf
  // 颜色预定义
  val ps1Conf = FirePS1Conf
  // 日期模式类型
  val dateSchemaConf = FireDateSchemaConf
  // 打印模块枚举
  val printModuleConf = FirePrintModuleConf
  // 常量字符串
  val stringConf = FireStringConf
  // log相关常量
  val logValConf = FireLogValConf
  // 预定义的一些正则表达式
  val regularsConf = FireRegularConf
  // 日志的级别
  val logLevelConf = FireLogLevelConf
  // hive相关配置
  val hiveConf = FireHiveConf
  // 预设状态
  val statusConf = FireStatusConf
  // HDFS配置
  val hdfsConf = FireHDFSConf

}

object FireConf extends FireConf {
  // 用于判断是否为spark引擎
  def isSparkEngine = "spark".equals(this.engine)

  // 用于判断是否为flink引擎
  def isFlinkEngine = "flink".equals(this.engine)
}