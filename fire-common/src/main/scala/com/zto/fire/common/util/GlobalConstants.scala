package com.zto.fire.common.util

import java.util

import com.zto.fire.common.conf._

import scala.collection.JavaConversions

/**
 * 常量配置类
 * Created by ChengLong on 2016-11-22.
 */
object GlobalConstants {

  // ---------------------- 用于Java中的配置信息 ---------------------- //
  // hbase默认的列族名称，如果使用FieldName指定，则会被覆盖
  lazy val familyName = PropUtils.getString(PropKeys.HBBASE_COLUMN_FAMILY_KEY, "info")
  // hbase操作失败最大重试次数
  lazy val hbaseMaxRetry = PropUtils.getLong(PropKeys.HBASE_MAX_RETRY, 3)
  // hbase集群名称
  lazy val hbaseCluster = PropUtils.getString(PropKeys.HBASE_CLUSTER_URL, "")
  // fire框架埋点日志开关
  lazy val fireLogEnable = FireConf.logEnable
  // 用于设置是否启用任务定时调度
  lazy val scheduleEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_TASK_SCHEDULE_ENABLE, true)
  // quartz最大线程池大小
  lazy val quartzMaxThread = PropUtils.getString(PropKeys.SPARK_FIRE_QUARTZ_MAX_THREAD, "8")
  // 定时任务黑名单，配置的value为方法名，多个以逗号分隔
  lazy val schedulerBlackList = PropUtils.getString(PropKeys.SPARK_FIRE_SCHEDULER_BLACKLIST, "")
  // hbase集群映射配置前缀
  lazy val hbaseClusterMapPrefix = "spark.hbase.cluster.map."
  // hbase集群映射地址
  lazy val hbaseClusterMap: util.Map[String, String] = JavaConversions.mapAsJavaMap(PropUtils.sliceKeys(hbaseClusterMapPrefix))
  // hbase java api 配置前缀
  lazy val hbaseConfPrefix = "spark.hbase.conf."
  // 用于区分不同的流计算引擎类型
  private[fire] lazy val engine = PropUtils.keyPrefix
  lazy val hbaseDurability = PropUtils.getString(PropKeys.HBASE_DURABILITY, "")

  // 用于判断是否为spark引擎
  def isSparkEngine = "spark".equals(this.engine)
  // 用于判断是否为flink引擎
  def isFlinkEngine = "flink".equals(this.engine)

  /**
   * 预定义的默认值，配置文件没有指明的情况下会取默认值
   */
  object DefaultVals extends DefaultValsConfextends

  /**
   * 对应conf.properties的key
   */
  object PropKeys extends PropKeysConfiguration

  /**
   * Fire框架相关配置
   */
  object FireConf extends FireConfiguration

  /**
   * flink相关配置
   */
  object FlinkConf extends FlinkConfiguration

  /**
   * 关系型数据库连接池相关配置
   */
  object JdbcConf extends JdbcConfiguration

  /**
   * Spark相关常量配置
   */
  object SparkConf extends SparkConfiguration

  /**
   * kafka相关配置
   */
  object KafkaConf extends KafkaConfiguration

  /**
   * rocketMQ相关配置
   */
  object RocketConf extends RocketConfiguration

  /**
   * hbase相关配置
   */
  object HBaseConf extends HBaseConfiguration

  /**
   * impala相关配置
   */
  object KuduConf extends KuduConfiguration


  /**
   * 周期相关字符串
   */
  object Cron extends CronConfiguration

  /**
   * 颜色预定义
   */
  object PS1 extends PS1Configuration

  /**
   * 日期模式类型
   */
  object DateTimeSchema extends DateTimeSchemaConfiguration

  /**
   * 打印模块枚举
   */
  object PrintModule extends PrintModuleConfiguration

  /**
   * 常量字符串
   */
  object Strings extends StringsConfiguration

  /**
   * log相关常量
   */
  object LogVal extends LogValConfiguration

  /**
   * 预定义的一些正则表达式
   */
  object Regulars extends RegularConfiguration

  /**
   * 日志的级别
   */
  object LogLevel extends LogLevelConfiguration

  /**
   * hive相关配置
   */
  object HiveConf extends HiveConfiguration

  /**
   * 预设状态
   */
  object Status extends StatusConfiguration

  /**
   * 用于定义累加日期的维度
   */
  object MultiTimerSchema extends MultiTimerSchemaConfiguration

  /**
   * HDFS配置
   */
  object HdfsConf extends HdfsConfiguration

}