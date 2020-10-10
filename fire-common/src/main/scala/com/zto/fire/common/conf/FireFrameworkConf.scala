package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang3.StringUtils

/**
 * Fire框架相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:54
 */
private[fire] object FireFrameworkConf {
  // fire版本号
  lazy val SPARK_FIRE_VERSION = "spark.fire.version"
  lazy val DRIVER_CLASS_NAME = "spark.driver.class.name"
  // fire内置线程池大小
  lazy val FIRE_THREAD_POOL_SIZE = "spark.fire.thread.pool.size"
  // fire内置定时任务线程池大小
  lazy val FIRE_THREAD_POOL_SCHEDULE_SIZE = "spark.fire.thread.pool.schedule.size"
  // 是否启用fire框架restful服务
  lazy val SPARK_FIRE_REST_ENABLE = "spark.fire.rest.enable"
  // rest接口权限认证
  lazy val SPARK_FIRE_REST_FILTER_ENABLE = "spark.fire.rest.filter.enable"
  // 用于配置是否关闭fire内置的所有累加器
  lazy val SPARK_FIRE_ACC_ENABLE = "spark.fire.acc.enable"
  // 日志累加器开关
  lazy val SPARK_FIRE_ACC_LOG_ENABLE = "spark.fire.acc.log.enable"
  // 多值累加器开关
  lazy val SPARK_FIRE_ACC_MULTI_COUNTER_ENABLE = "spark.fire.acc.multi.counter.enable"
  // 多时间维度累加器开关
  lazy val SPARK_FIRE_ACC_MULTI_TIMER_ENABLE = "spark.fire.acc.multi.timer.enable"
  // env累加器开关
  lazy val SPARK_FIRE_ACC_ENV_ENABLE = "spark.fire.acc.env.enable"
  // fire框架埋点日志开关，当关闭后，埋点的日志将不再被记录到日志累加器中，并且也不再打印
  lazy val SPARK_FIRE_LOG_ENABLE = "spark.fire.log.enable"
  // 用于限定fire框架中sql日志的字符串长度
  lazy val SPARK_FIRE_LOG_SQL_LENGTH = "spark.fire.log.sql.length"
  // fire框架rest接口服务最大线程数
  lazy val SPARK_FIRE_RESTFUL_MAX_THREAD = "spark.fire.restful.max.thread"
  // 用于配置是否抛弃zrc独立运行，配置为false表示不向zrc注册，不获取zrc配置
  lazy val SPARK_FIRE_ZRC_ENABLE = "spark.fire.zrc.enable"
  // zrc接口调用秘钥
  lazy val SPARK_FIRE_ZRC_SECRET = "spark.fire.zrc.register.conf.secret"
  // fire框架restful端口冲突重试次数
  lazy val SPARK_FIRE_RESTFUL_PORT_RETRY_NUM = "spark.fire.restful.port.retry_num"
  // fire框架restful端口冲突重试时间（ms）
  lazy val SPARK_FIRE_RESTFUL_PORT_RETRY_DURATION = "spark.fire.restful.port.retry_duration"
  lazy val SPARK_LOG_LEVEL_CONF_PREFIX = "spark.fire.log.level.conf."
  // 日志记录器保留最少的记录数
  lazy val SPARK_FIRE_ACC_LOG_MIN_SIZE = "spark.fire.acc.log.min.size"
  // 日志记录器保留最多的记录数
  lazy val SPARK_FIRE_ACC_LOG_MAX_SIZE = "spark.fire.acc.log.max.size"
  // env累加器保留最多的记录数
  lazy val SPARK_FIRE_ACC_ENV_MAX_SIZE = "spark.fire.acc.env.max.size"
  // env累加器保留最少的记录数
  lazy val SPARK_FIRE_ACC_ENV_MIN_SIZE = "spark.fire.acc.env.min.size"
  // timer累加器保留最大的记录数
  lazy val SPARK_FIRE_ACC_TIMER_MAX_SIZE = "spark.fire.acc.timer.max.size"
  // timer累加器清理几小时之前的记录
  lazy val SPARK_FIRE_ACC_TIMER_MAX_HOUR = "spark.fire.acc.timer.max.hour"
  // 定时调度任务黑名单（定时任务方法名），以逗号分隔
  lazy val SPARK_FIRE_SCHEDULER_BLACKLIST = "spark.fire.scheduler.blacklist"
  // 用于配置是否启用任务定时调度
  lazy val SPARK_FIRE_TASK_SCHEDULE_ENABLE = "spark.fire.task.schedule.enable"
  // quartz最大线程池大小
  lazy val SPARK_FIRE_QUARTZ_MAX_THREAD = "spark.fire.quartz.max.thread"
  // fire框架restful地址
  def fireRestUrl(engine: String = "spark"): String = s"$engine.fire.rest.url"
  // zrc生产环境注册地址
  lazy val SPARK_ZRC_REGISTER_CONF_PROD_ADDRESS = "spark.fire.zrc.register.conf.prod.address"
  // zrc测试环境注册地址
  lazy val SPARK_ZRC_REGISTER_CONF_TEST_ADDRESS = "spark.fire.zrc.register.conf.test.address"
  // spark streaming的remember时间，-1表示不生效(ms)
  lazy val SPARK_FIRE_STREAMING_REMEMBER = "spark.fire.streaming.remember"
  // 配置打印黑名单，配置项以逗号分隔
  lazy val SPARK_FIRE_CONF_PRINT_BLACKLIST = "spark.fire.conf.print.blacklist"
  // 是否启用动态配置功能
  lazy val SPARK_FIRE_DYNAMIC_CONF_ENABLE = "spark.fire.dynamic.conf.enable"
  // 是否打印配置信息
  lazy val SPARK_FIRE_CONF_SHOW_ENABLE = "spark.fire.conf.show.enable"
  // 是否将fire restful地址以日志形式打印
  lazy val SPARK_FIRE_REST_URL_SHOW_ENABLE = "spark.fire.rest.url.show.enable"
  // 各引擎单独配置文件名称（省略扩展名.properties）
  lazy val FIRE_CONF_FILE = "fire"
  lazy val SPARK_CONF_FILE = "spark"
  lazy val SPARK_STREAMING_CONF_FILE = "spark-streaming"
  lazy val SPARK_STRUCTURED_STREAMING_CONF_FILE = "structured-streaming"
  lazy val SPARK_CORE_CONF_FILE = "spark-core"
  lazy val FLINK_CONF_FILE = "flink"
  lazy val FLINK_STREAMING_CONF_FILE = "flink-streaming"
  lazy val FLINK_BATCH_CONF_FILE = "flink-batch"
  lazy val FIRE_DEPLOY_CONF_ENABLE = "spark.fire.deploy_conf.enable"

  // 是否将配置同步到executor、taskmanager端
  lazy val deployConf = PropUtils.getBoolean(this.FIRE_DEPLOY_CONF_ENABLE, true)

  // fire内置线程池大小
  lazy val threadPoolSize = PropUtils.getInt(this.FIRE_THREAD_POOL_SIZE, 5)
  // fire内置定时任务线程池大小
  lazy val threadPoolSchedulerSize = PropUtils.getInt(this.FIRE_THREAD_POOL_SCHEDULE_SIZE, 5)

  // fire日志打印黑名单
  lazy val fireConfBlackList: Set[String] = {
    val blacklist = PropUtils.getString(this.SPARK_FIRE_CONF_PRINT_BLACKLIST, "")
    if (StringUtils.isNotBlank(blacklist)) blacklist.split(",").toSet else Set.empty
  }

  // 获取driver的class name
  lazy val driverClassName = PropUtils.getString(this.DRIVER_CLASS_NAME)
  // 是否打印配置信息
  lazy val fireConfShow: Boolean = PropUtils.getBoolean(this.SPARK_FIRE_CONF_SHOW_ENABLE, true)
  // 是否将restful地址以日志方式打印
  lazy val fireRestUrlShow: Boolean = PropUtils.getBoolean(this.SPARK_FIRE_REST_URL_SHOW_ENABLE, false)
  // 获取动态配置参数
  lazy val dynamicConf: Boolean = PropUtils.getBoolean(this.SPARK_FIRE_DYNAMIC_CONF_ENABLE, true)
  // spark streaming的remember时间，-1表示不生效(ms)
  def streamingRemember: Long = PropUtils.getLong(this.SPARK_FIRE_STREAMING_REMEMBER, -1)
  // 用于获取fire版本号
  lazy val fireVersion = PropUtils.getString(this.SPARK_FIRE_VERSION, "1.0.0")
  // quartz最大线程池大小
  lazy val quartzMaxThread = PropUtils.getString(this.SPARK_FIRE_QUARTZ_MAX_THREAD, "8")
  // 用于设置是否启用任务定时调度
  lazy val scheduleEnable = PropUtils.getBoolean(this.SPARK_FIRE_TASK_SCHEDULE_ENABLE, true)
  // 定时任务黑名单，配置的value为方法名，多个以逗号分隔
  def schedulerBlackList: String = PropUtils.getString(this.SPARK_FIRE_SCHEDULER_BLACKLIST, "")
  // env累加器开关
  lazy val accEnvEnable = PropUtils.getBoolean(this.SPARK_FIRE_ACC_ENV_ENABLE, true)
  // 是否启用Fire内置的restful服务
  lazy val restEnable = PropUtils.getBoolean(this.SPARK_FIRE_REST_ENABLE, true)
  // rest接口权限认证
  lazy val restFilter = PropUtils.getBoolean(this.SPARK_FIRE_REST_FILTER_ENABLE, true)
  // 是否关闭fire内置的所有累加器
  lazy val accEnable = PropUtils.getBoolean(this.SPARK_FIRE_ACC_ENABLE, true)
  // 日志累加器开关
  lazy val accLogEnable = PropUtils.getBoolean(this.SPARK_FIRE_ACC_LOG_ENABLE, true)
  // 多值累加器开关
  lazy val accMultiCounterEnable = PropUtils.getBoolean(this.SPARK_FIRE_ACC_MULTI_COUNTER_ENABLE, true)
  // 多时间维度累加器开关
  lazy val accMultiTimerEnable = PropUtils.getBoolean(this.SPARK_FIRE_ACC_MULTI_TIMER_ENABLE, true)
  // fire框架埋点日志开关
  lazy val logEnable = PropUtils.getBoolean(this.SPARK_FIRE_LOG_ENABLE, true)
  // 用于限定fire框架中sql日志的字符串长度
  lazy val logSqlLength = PropUtils.getInt(this.SPARK_FIRE_LOG_SQL_LENGTH, 50)
  // zrc生产环境注册地址
  lazy val zrcProdAddress = PropUtils.getString(this.SPARK_ZRC_REGISTER_CONF_PROD_ADDRESS, "http://192.168.33.199:8080/zrcToExternal/zrcConfCallBack")
  // zrc测试环境注册地址
  lazy val zrcTestAddress = PropUtils.getString(this.SPARK_ZRC_REGISTER_CONF_TEST_ADDRESS)


  // fire框架rest接口服务最大线程数
  lazy val restfulMaxThread = PropUtils.getInt(this.SPARK_FIRE_RESTFUL_MAX_THREAD, 8)
  // 用于配置是否抛弃zrc独立运行，配置为false表示不向zrc注册，不获取zrc配置
  lazy val zrcEnable = PropUtils.getBoolean(this.SPARK_FIRE_ZRC_ENABLE, true)
  // zrc接口调用秘钥
  lazy val zrcSecret = PropUtils.getString(this.SPARK_FIRE_ZRC_SECRET, "21fa30b7f2082b1b12dfbc7c8c6d70b9")
  // fire框架restful端口冲突重试次数
  lazy val restfulPortRetryNum = PropUtils.getInt(this.SPARK_FIRE_RESTFUL_PORT_RETRY_NUM, 3)
  // fire框架restful端口冲突重试时间（ms）
  lazy val restfulPortRetryDuration = PropUtils.getLong(this.SPARK_FIRE_RESTFUL_PORT_RETRY_DURATION, 1000L)
  // 用于限定日志最少保存量，防止当日志量达到maxLogSize时频繁的进行clear操作
  lazy val minLogSize = PropUtils.getInt(this.SPARK_FIRE_ACC_LOG_MIN_SIZE, 500).abs
  // 用于限定日志最大保存量，防止日志量过大，撑爆driver
  lazy val maxLogSize = PropUtils.getInt(this.SPARK_FIRE_ACC_LOG_MAX_SIZE, 1000).abs
  // 用于限定运行时信息最少保存量，防止当运行时信息量达到maxEnvSize时频繁的进行clear操作
  lazy val minEnvSize = PropUtils.getInt(this.SPARK_FIRE_ACC_ENV_MIN_SIZE, 100).abs
  // 用于限定运行时信息最大保存量，防止过大撑爆driver
  lazy val maxEnvSize = PropUtils.getInt(this.SPARK_FIRE_ACC_ENV_MAX_SIZE, 500).abs
  // 用于限定最大保存量，防止数据量过大，撑爆driver
  lazy val maxTimerSize = PropUtils.getInt(this.SPARK_FIRE_ACC_TIMER_MAX_SIZE, 1000).abs
  // 用于指定清理指定小时数之前的记录
  lazy val maxTimerHour = PropUtils.getInt(this.SPARK_FIRE_ACC_TIMER_MAX_HOUR, 12).abs
}