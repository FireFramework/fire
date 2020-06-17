package com.zto.fire.common.util

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Durability

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
  lazy val hbaseCluster = PropUtils.getString(PropKeys.HBASE_CLUSTER_URL, DefaultVals.hbaseName)
  // fire框架埋点日志开关
  lazy val fireLogEnable = FireConf.logEnable
  // 用于设置是否启用任务定时调度
  lazy val scheduleEnable = PropUtils.getBoolean(PropKeys.SPARK_FIRE_TASK_SCHEDULE_ENABLE, true)
  // quartz最大线程池大小
  lazy val quartzMaxThread = PropUtils.getString(PropKeys.SPARK_FIRE_QUARTZ_MAX_THREAD, "8")
  // 定时任务黑名单，配置的value为方法名，多个以逗号分隔
  lazy val schedulerBlackList = PropUtils.getString(PropKeys.SPARK_FIRE_SCHEDULER_BLACKLIST, "")
  // 用于区分不同的流计算引擎类型
  private[fire] lazy val engine = PropUtils.keyPrefix

  /**
   * 用于判断是否为spark引擎
   */
  def isSparkEngine = "spark".equals(this.engine)

  /**
   * 用于判断是否为flink引擎
   */
  def isFlinkEngine = "flink".equals(this.engine)

  /**
   * 预定义的默认值，配置文件没有指明的情况下会取默认值
   */
  object DefaultVals extends Enumeration {
    // hbase集群名称标识
    val hbaseName = "batch"

    // rest接口filter的开关
    val restFilter = true

    // 默认的broker名称
    val kafkaBrokersName = "bigdata"
    // 启动应用时默认的kafka消费位点
    val kafkaStartingOffset = KafkaConf.offsetLargest
    // 数据丢失时执行失败
    val kafkaFailOnDataLoss = true
    // enable.auto.commit
    val kafkaEnableAutoCommit = false
    // 默认的事务隔离级别
    val jdbcIsolationLevel = "READ_UNCOMMITTED"
    // 数据库批量操作的记录数
    val jdbcBatchSize = 1000

    // 启动应用时默认的rocket消费位点
    val rocketStartingOffset = RocketConf.rocketOffsetLargest
    // 数据丢失时执行失败
    val rocketFailOnDataLoss = true
    // enable.auto.commit
    val rocketEnableAutoCommit = false
    // 订阅的tag
    val rocketConsumerTag = "*"

    // spark 默认的checkpoint地址
    val sparkChkPointDir = "hdfs://nameservice1/user/spark/ckpoint/"
    // hive metastore地址
    val hiveCluster = "batch"
    // 默认的日志级别
    val logLevel = LogLevel.INFO
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
    val logSqlLength = 100
  }

  /**
   * 对应conf.properties的key
   */
  object PropKeys extends Enumeration {
    // 运行模式
    val RUNMODEL_KEY = "spark.runModel"
    val APP_NAME_KEY = "spark.appName"
    val SPARK_CONF_KEY = "SparkConf"
    val SPARK_LOCAL_CORES = "spark.local.cores"

    // c3p0连接池相关配置
    val SPARK_DB_JDBC_URL_KEY = "spark.db.jdbc.url"
    val SPARK_DB_JDBC_DRIVER_KEY = "spark.db.jdbc.driver"
    val SPARK_DB_JDBC_USER_KEY = "spark.db.jdbc.user"
    val SPARK_DB_JDBC_PASSWORD_KEY = "spark.db.jdbc.password"
    val SPARK_DB_JDBC_ISOLATION_LEVEL = "spark.db.jdbc.isolation.level"
    val SPARK_DB_JDBC_MAX_POOL_SIZE_KEY = "spark.db.jdbc.maxPoolSize"
    val SPARK_DB_JDBC_MIN_POOL_SIZE_KEY = "spark.db.jdbc.minPoolSize"
    val SPARK_DB_JDBC_ACQUIRE_INCREMENT_KEY = "spark.db.jdbc.acquireIncrement"
    val SPARK_DB_JDBC_INITIAL_POOL_SIZE_KEY = "spark.db.jdbc.initialPoolSize"
    val SPARK_DB_JDBC_MAX_IDLE_TIME_KEY = "spark.db.jdbc.maxIdleTime"
    val SPARK_DB_JDBC_BATCH_SIZE = "spark.db.jdbc.batch.size"
    val SPARK_DB_JDBC_FLUSH_INTERVAL = "spark.db.jdbc.flushInterval"
    val SPARK_DB_JDBC_MAX_RETRY = "spark.db.jdbc.max.retry"

    val LOG_LEVEL = "spark.log.level"
    val SAVE_MODE_KEY = "spark.saveMode"
    val PARALLELISM_KEY = "spark.parallelism"
    val HBBASE_COLUMN_FAMILY_KEY = "spark.hbase.column.family"
    val HBASE_MAX_RETRY = "spark.hbase.max.retry"
    val HbaseDurability_KEY = "HbaseDurability"
    val KUDU_MASTER_URL = "spark.kudu.master"
    val HBASE_CLUSTER_URL = "spark.hbase.cluster"
    val HBASE_BATCH = "spark.hbase.batch.size"
    val IMPALA_CONNECTION_URL_KEY: String = "spark.impala.connection.url"
    val IMPALA_JDBC_DRIVER_NAME_KEY: String = "spark.impala.jdbc.driver.class.name"
    val IMPALA_DAEMONS_URL = "spark.impala.daemons.url"

    // ---------------------------- kafka 相关配置 ---------------------------- //
    val KAFKA_BROKERS_NAME = "spark.kafka.brokers.name"
    // kafka的topic列表，以逗号分隔
    val KAFKA_TOPICS = "spark.kafka.topics"
    // kafka起始消费位点
    val KAFKA_STARTING_OFFSET = "spark.kafka.starting.offsets"
    // kafka结束消费位点
    val KAFKA_ENDING_OFFSET = "spark.kafka.ending.offsets"
    // 从Kafka轮询数据的超时时间（以毫秒为单位）
    val KAFKA_POLL_TIMEOUT_MS = "spark.kafka.poll.timeout.ms"
    // 放弃获取Kafka偏移前重试的次数
    val KAFKA_FETCH_OFFSET_NUM_RETRIES = "spark.kafka.fetch.offset.num.retries"
    // 重试获取Kafka偏移之前要等待的毫秒数
    val KAFKA_FETCH_OFFSET_RETRY_INTERVAL_MS = "spark.kafka.fetch.offset.retry.interval.ms"
    // 每个触发间隔处理的最大偏移量的速率限制，指定的偏移总数将在不同卷的topicPartitions中按比例分配
    val KAFKA_MAX_OFFSETS_PER_TRIGGER = "spark.kafka.max.offsets.per.trigger"
    // 丢失数据是否失败
    val KAFKA_FAIL_ON_DATA_LOSS = "spark.kafka.failOnDataLoss"
    // 是否自动维护offset
    val KAFKA_ENABLE_AUTO_COMMIT = "spark.kafka.enable.auto.commit"
    // group.id
    val KAFKA_GROUP_ID = "spark.kafka.group.id"
    // kafka session超时时间
    val KAFKA_SESSION_TIMEOUT_MS = "spark.kafka.session.timeout.ms"
    // kafka request超时时间
    val KAFKA_REQUEST_TIMEOUT_MS = "spark.kafka.request.timeout.ms"
    val KAFKA_MAX_POLL_INTERVAL_MS = "spark.kafka.max.poll.interval.ms"
    // 心跳间隔时间：heartbeat.interval.ms
    val KAFKA_HEARTBEAT_INTERVAL_MS = "spark.kafka.heartbeat.interval.ms"
    // 消费者组最大的session超时时间：group.max.session.timeout.ms
    val KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS = "spark.kafka.group.max.session.timeout.ms"
    // 消费者组最小的session超时时间：group.min.session.timeout.ms
    val KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS = "spark.kafka.group.min.session.timeout.ms"
    // 一次调用pool返回的最大记录数：max.poll.records
    val KAFKA_MAX_POLL_RECORDS = "spark.kafka.max.poll.records"
    // 每个分区返回的最大数据量：max.partition.fetch.bytes
    val KAFKA_MAX_PARTITION_FETCH_BYTES = "spark.kafka.max.partition.fetch.bytes"

    // ---------------------------- spark 相关配置 ---------------------------- //
    val SPARK_CHK_POINT_DIR = "spark.chkpoint.dir"
    val SPARK_LOG_LEVEL = "spark.log.level"
    // spark streaming批次时间
    val SPARK_STREAMING_BATCH_DURATION = "spark.streaming.batch.duration"

    // ---------------------------- hive 相关配置 ---------------------------- //
    val HIVE_SUPPORT_ENABLE = "spark.hive.support.enable"
    val HIVE_CLUSTER = "spark.hive.cluster"
    // 默认的库名
    val SPARK_DEFAULT_DATABASE_NAME = "spark.default.database.name"
    // 默认的分区名称
    val SPARK_DEFAULT_TABLE_PARTITION_NAME = "spark.default.table.partition.name"
    // hive-site.xml配置文件存放的路径
    val HIVE_SITE_DIR = "spark.hive.conf.dir"
    // hive版本号
    val HIVE_VERSION = "spark.hive.version"
    // hive的catalog名称
    val HIVE_CATALOG_NAME = "spark.hive.catalog.name"

    // ---------------------------- RocketMQ 相关配置 ---------------------------- //
    // rocketMQ name server
    val ROCKET_BROKERS_NAME = "spark.rocket.brokers.name"
    // rocketMQ topic信息，多个以逗号分隔
    val ROCKET_TOPICS = "spark.rocket.topics"
    // rocketMQ groupId
    val ROCKET_GROUP_ID = "spark.rocket.group.id"
    // 丢失数据是否失败
    val ROCKET_FAIL_ON_DATA_LOSS = "spark.rocket.failOnDataLoss"
    // 是否自动维护offset
    val ROCKET_ENABLE_AUTO_COMMIT = "spark.rocket.enable.auto.commit"
    // RocketMQ起始消费位点
    val ROCKET_STARTING_OFFSET = "spark.rocket.starting.offsets"
    // rocketMq订阅的tag
    val ROCKET_CONSUMER_TAG = "spark.rocket.consumer.tag"
    val ROCKET_NAMESERVER_POLL_INTERVAL = "spark.rocket.nameserver.poll.interval"
    val ROCKET_BROKERSERVER_HEARTBEAT_INTERVAL = "spark.rocket.brokerserver.heartbeat.interval"
    val ROCKET_CONSUMER_OFFSET_PERSIST_INTERVAL = "spark.rocket.consumer.offset.persist.interval"
    val ROCKET_CONSUMER_MIN_THREADS = "spark.rocket.consumer.min.threads"
    val ROCKET_CONSUMER_MAX_THREADS = "spark.rocket.consumer.max.threads"
    val ROCKET_SPOUT_MESSAGES_MAX_RETRY = "spark.rocket.spout.messages.max.retry"
    val ROCKET_PULL_MAX_SPEED_PER_PARTITION = "spark.rocket.pull.max.speed.per.partition"
    val ROCKET_PULL_MAX_BATCH_SIZE = "spark.rocket.pull.max.batch.size"
    val ROCKET_PULL_TIMEOUT_MS = "spark.rocket.pull.timeout.ms"

    // ---------------------------- Fire 相关配置 ---------------------------- //
    // 日志记录器保留最少的记录数
    val SPARK_FIRE_ACC_LOG_MIN_SIZE = "spark.fire.acc.log.min.size"
    // 日志记录器保留最多的记录数
    val SPARK_FIRE_ACC_LOG_MAX_SIZE = "spark.fire.acc.log.max.size"
    // env累加器保留最多的记录数
    val SPARK_FIRE_ACC_ENV_MAX_SIZE = "spark.fire.acc.env.max.size"
    // env累加器保留最少的记录数
    val SPARK_FIRE_ACC_ENV_MIN_SIZE = "spark.fire.acc.env.min.size"
    // timer累加器保留最大的记录数
    val SPARK_FIRE_ACC_TIMER_MAX_SIZE = "spark.fire.acc.timer.max.size"
    // timer累加器清理几小时之前的记录
    val SPARK_FIRE_ACC_TIMER_MAX_HOUR = "spark.fire.acc.timer.max.hour"
    // rest接口权限认证
    val SPARK_FIRE_REST_FILTER_ENABLE = "spark.fire.rest.filter.enable"
    // 用于配置是否关闭fire内置的所有累加器
    val SPARK_FIRE_ACC_ENABLE = "spark.fire.acc.enable"
    // 日志累加器开关
    val SPARK_FIRE_ACC_LOG_ENABLE = "spark.fire.acc.log.enable"
    // 多值累加器开关
    val SPARK_FIRE_ACC_MULTI_COUNTER_ENABLE = "spark.fire.acc.multi.counter.enable"
    // 多时间维度累加器开关
    val SPARK_FIRE_ACC_MULTI_TIMER_ENABLE = "spark.fire.acc.multi.timer.enable"
    // env累加器开关
    val SPARK_FIRE_ACC_ENV_ENABLE = "spark.fire.acc.env.enable"
    // fire框架埋点日志开关，当关闭后，埋点的日志将不再被记录到日志累加器中，并且也不再打印
    val SPARK_FIRE_LOG_ENABLE = "spark.fire.log.enable"
    // 用于限定fire框架中sql日志的字符串长度
    val SPARK_FIRE_LOG_SQL_LENGTH = "spark.fire.log.sql.length"
    // fire框架针对hbase操作后数据集的缓存策略，配置列表详见：StorageLevel.scala（配置不区分大小写）
    val SPARK_FIRE_HBASE_STORAGE_LEVEL = "spark.fire.hbase.storage.level"
    // 通过HBase scan后repartition的分区数
    val SPARK_FIRE_HBASE_SCAN_REPARTITIONS = "spark.fire.hbase.scan.repartitions"

    // fire框架针对jdbc操作后数据集的缓存策略
    val SPARK_FIRE_JDBC_STORAGE_LEVEL = "spark.fire.jdbc.storage.level"
    // 通过JdbcOper查询后将数据集放到多少个分区中，需根据实际的结果集做配置
    val SPARK_FIRE_JDBC_QUERY_REPARTITIONS = "spark.fire.jdbc.query.partitions"
    // 用于配置是否启用任务定时调度
    val SPARK_FIRE_TASK_SCHEDULE_ENABLE = "spark.fire.task.schedule.enable"
    // fire框架rest接口服务最大线程数
    val SPARK_FIRE_RESTFUL_MAX_THREAD = "spark.fire.restful.max.thread"
    // quartz最大线程池大小
    val SPARK_FIRE_QUARTZ_MAX_THREAD = "spark.fire.quartz.max.thread"
    // 定时调度任务黑名单（定时任务方法名），以逗号分隔
    val SPARK_FIRE_SCHEDULER_BLACKLIST = "spark.fire.scheduler.blacklist"
    // 用于配置是否抛弃zrc独立运行，配置为false表示不向zrc注册，不获取zrc配置
    val SPARK_FIRE_ZRC_ENABLE = "spark.fire.zrc.enable"
    // fire框架restful端口冲突重试次数
    val SPARK_FIRE_RESTFUL_PORT_RETRY_NUM = "spark.fire.restful.port.retry_num"
    // fire框架restful端口冲突重试时间（ms）
    val SPARK_FIRE_RESTFUL_PORT_RETRY_DURATION = "spark.fire.restful.port.retry_duration"

    // ---------------------------- HDFS 相关配置 ---------------------------- //
    // 是否启用高可用
    val HDFS_HA = "spark.hdfs.ha.enable"

    // ---------------------------- FLINK 相关配置 ---------------------------- //
    val FLINK_AUTO_GENERATE_UID_ENABLE = "flink.auto.generate.uid.enable"
    val FLINK_AUTO_TYPE_REGISTRATION_ENABLE = "flink.auto.type.registration.enable"
    val FLINK_FORCE_AVRO_ENABLE = "flink.force.avro.enable"
    val FLINK_FORCE_KRYO_ENABLE = "flink.force.kryo.enable"
    val FLINK_GENERIC_TYPES_ENABLE = "flink.generic.types.enable"
    val FLINK_OBJECT_REUSE_ENABLE = "flink.object.reuse.enable"
    val FLINK_AUTO_WATERMARK_INTERVAL = "flink.auto.watermark.interval"
    val FLINK_CLOSURE_CLEANER_LEVEL = "flink.closure.cleaner.level"
    val FLINK_DEFAULT_INPUT_DEPENDENCY_CONSTRAINT = "flink.default.input.dependency.constraint"
    val FLINK_EXECUTION_MODE = "flink.execution.mode"
    val FLINK_LATENCY_TRACKING_INTERVAL = "flink.latency.tracking.interval"
    val FLINK_MAX_PARALLELISM = "flink.max.parallelism"
    val FLINK_DEFAULT_PARALLELISM = "flink.default.parallelism"
    val FLINK_TASK_CANCELLATION_INTERVAL = "flink.task.cancellation.interval"
    val FLINK_TASK_CANCELLATION_TIMEOUT_MILLIS = "flink.task.cancellation.timeout.millis"
    val FLINK_USE_SNAPSHOT_COMPRESSION = "flink.use.snapshot.compression"
    val FLINK_STREAM_BUFFER_TIMEOUT_MILLIS = "flink.stream.buffer.timeout.millis"
    val FLINK_STREAM_NUMBER_EXECUTION_RETRIES = "flink.stream.number.execution.retries"
    val FLINK_STREAM_TIME_CHARACTERISTIC = "flink.stream.time.characteristic"
    // checkpoint相关配置项
    val FLINK_STREAM_CHECKPOINT_INTERVAL = "flink.stream.checkpoint.interval"
    val FLINK_STREAM_CHECKPOINT_MODE = "flink.stream.checkpoint.mode"
    val FLINK_STREAM_CHECKPOINT_TIMEOUT = "flink.stream.checkpoint.timeout"
    val FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT = "flink.stream.checkpoint.max.concurrent"
    val FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN = "flink.stream.checkpoint.min.pause.between"
    val FLINK_STREAM_CHECKPOINT_PREFER_RECOVERY = "flink.stream.checkpoint.prefer.recovery"
    val FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER = "flink.stream.checkpoint.tolerable.failure.number"
    val FLINK_STREAM_CHECKPOINT_EXTERNALIZED = "flink.stream.checkpoint.externalized"
  }

  /**
   * Fire框架相关配置
   */
  object FireConf extends Enumeration {
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
    // fire框架restful端口冲突重试次数
    lazy val restfulPortRetryNum = PropUtils.getInt(PropKeys.SPARK_FIRE_RESTFUL_PORT_RETRY_NUM, 3)
    // fire框架restful端口冲突重试时间（ms）
    lazy val restfulPortRetryDuration = PropUtils.getLong(PropKeys.SPARK_FIRE_RESTFUL_PORT_RETRY_DURATION, 1000L)

    /**
     * 获取配置的HBase缓存策略
     */
    def hbaseStorageLevel: String = hbaseStorageLevelConf

    /**
     * 获取配置的JDBC缓存策略
     */
    def jdbcStorageLevel: String = jdbcStorageLevelConf
  }

  /**
   * flink相关配置
   */
  object FlinkConf extends Enumeration {
    lazy val autoGenerateUidEnable = PropUtils.getBoolean(GlobalConstants.PropKeys.FLINK_AUTO_GENERATE_UID_ENABLE, true)
    lazy val autoTypeRegistrationEnable = PropUtils.getBoolean(GlobalConstants.PropKeys.FLINK_AUTO_TYPE_REGISTRATION_ENABLE, true)
    lazy val forceAvroEnable = PropUtils.getBoolean(GlobalConstants.PropKeys.FLINK_FORCE_AVRO_ENABLE, false)
    lazy val forceKryoEnable = PropUtils.getBoolean(GlobalConstants.PropKeys.FLINK_FORCE_KRYO_ENABLE, false)
    lazy val genericTypesEnable = PropUtils.getBoolean(GlobalConstants.PropKeys.FLINK_GENERIC_TYPES_ENABLE, false)
    lazy val objectReuseEnable = PropUtils.getBoolean(GlobalConstants.PropKeys.FLINK_OBJECT_REUSE_ENABLE, false)
    lazy val autoWatermarkInterval = PropUtils.getLong(GlobalConstants.PropKeys.FLINK_AUTO_WATERMARK_INTERVAL)
    lazy val closureCleanerLevel = PropUtils.getString(GlobalConstants.PropKeys.FLINK_CLOSURE_CLEANER_LEVEL)
    lazy val defaultInputDependencyConstraint = PropUtils.getString(GlobalConstants.PropKeys.FLINK_DEFAULT_INPUT_DEPENDENCY_CONSTRAINT)
    lazy val executionMode = PropUtils.getString(GlobalConstants.PropKeys.FLINK_EXECUTION_MODE)
    lazy val latencyTrackingInterval = PropUtils.getLong(GlobalConstants.PropKeys.FLINK_LATENCY_TRACKING_INTERVAL, -1)
    lazy val maxParallelism = PropUtils.getInt(GlobalConstants.PropKeys.FLINK_MAX_PARALLELISM, 8)
    lazy val defaultParallelism = PropUtils.getInt(GlobalConstants.PropKeys.FLINK_DEFAULT_PARALLELISM, -1)
    lazy val taskCancellationInterval = PropUtils.getLong(GlobalConstants.PropKeys.FLINK_TASK_CANCELLATION_INTERVAL, -1)
    lazy val taskCancellationTimeoutMillis = PropUtils.getLong(GlobalConstants.PropKeys.FLINK_TASK_CANCELLATION_TIMEOUT_MILLIS, -1)
    lazy val useSnapshotCompression = PropUtils.getBoolean(GlobalConstants.PropKeys.FLINK_USE_SNAPSHOT_COMPRESSION, false)
    lazy val streamBufferTimeoutMillis = PropUtils.getLong(GlobalConstants.PropKeys.FLINK_STREAM_BUFFER_TIMEOUT_MILLIS, -1)
    lazy val streamNumberExecutionRetries = PropUtils.getInt(GlobalConstants.PropKeys.FLINK_STREAM_NUMBER_EXECUTION_RETRIES, -1)
    lazy val streamTimeCharacteristic = PropUtils.getString(GlobalConstants.PropKeys.FLINK_STREAM_TIME_CHARACTERISTIC, "")

    // checkpoint相关配置项
    lazy val streamCheckpointInterval = PropUtils.getLong(GlobalConstants.PropKeys.FLINK_STREAM_CHECKPOINT_INTERVAL, -1)
    lazy val streamCheckpointMode = PropUtils.getString(GlobalConstants.PropKeys.FLINK_STREAM_CHECKPOINT_MODE, "EXACTLY_ONCE")
    lazy val streamCheckpointTimeout = PropUtils.getLong(GlobalConstants.PropKeys.FLINK_STREAM_CHECKPOINT_TIMEOUT, 600000L)
    lazy val streamCheckpointMaxConcurrent = PropUtils.getInt(GlobalConstants.PropKeys.FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, 1)
    lazy val streamCheckpointMinPauseBetween = PropUtils.getInt(GlobalConstants.PropKeys.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, 0)
    lazy val streamCheckpointPreferRecovery = PropUtils.getBoolean(GlobalConstants.PropKeys.FLINK_STREAM_CHECKPOINT_PREFER_RECOVERY, false)
    lazy val streamCheckpointTolerableTailureNumber = PropUtils.getInt(GlobalConstants.PropKeys.FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, 0)
    lazy val streamCheckpointExternalized = PropUtils.getString(GlobalConstants.PropKeys.FLINK_STREAM_CHECKPOINT_EXTERNALIZED, "RETAIN_ON_CANCELLATION")
  }

  /**
   * 关系型数据库连接池相关配置
   */
  object JdbcConf extends Enumeration {
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

  /**
   * Spark相关常量配置
   */
  object SparkConf extends Enumeration {
    val appName = PropUtils.getString(PropKeys.APP_NAME_KEY, "")
    val localCores = PropUtils.getString(PropKeys.SPARK_LOCAL_CORES, "*")
    val sparkConf = PropUtils.getString(PropKeys.SPARK_CONF_KEY)
    val logLevel = PropUtils.getString(PropKeys.LOG_LEVEL, DefaultVals.logLevel).toUpperCase
    val saveMode = PropUtils.getString(PropKeys.SAVE_MODE_KEY, "Append")
    val parallelism = PropUtils.getInt(PropKeys.PARALLELISM_KEY)
    val chkPointDirPrefix = PropUtils.getString(PropKeys.SPARK_CHK_POINT_DIR, DefaultVals.sparkChkPointDir)
    val defaultDB = PropUtils.getString(PropKeys.SPARK_DEFAULT_DATABASE_NAME, DefaultVals.dbName)
    val partitionName = PropUtils.getString(PropKeys.SPARK_DEFAULT_TABLE_PARTITION_NAME, DefaultVals.partitionName)
  }

  /**
   * kafka相关配置
   */
  object KafkaConf extends Enumeration {
    val offsetLargest = "latest"
    val offsetSmallest = "earliest"
    val offsetNone = "none"

    // 初始化kafka集群名称与地址映射
    private lazy val kafkaMap = PropUtils.sliceKeys("spark.kafka.cluster.map.")

    // kafka消费起始位点
    def kafkaStartingOffset(keyNum: Int = 1): String = PropUtils.getString(PropKeys.KAFKA_STARTING_OFFSET, keyNum, DefaultVals.kafkaStartingOffset)

    // kafka消费结束位点
    def kafkaEndingOffsets(keyNum: Int = 1): String = PropUtils.getString(PropKeys.KAFKA_ENDING_OFFSET, keyNum, "")

    // 从Kafka轮询数据的超时时间（以毫秒为单位，默认1024）
    def kafkaPollTimeoutMs(keyNum: Int = 1): Long = PropUtils.getLong(PropKeys.KAFKA_POLL_TIMEOUT_MS, keyNum, 1024)

    // 放弃获取Kafka偏移前重试的次数，默认3次
    def kafkaFetchOffsetNumRetries(keyNum: Int = 1): Int = PropUtils.getInt(PropKeys.KAFKA_FETCH_OFFSET_NUM_RETRIES, keyNum, 3)

    // 重试获取Kafka偏移之前要等待的毫秒数，默认10毫秒
    def kafkaFetchOffsetRetryIntervalMs(keyNum: Int = 1): Long = PropUtils.getLong(PropKeys.KAFKA_FETCH_OFFSET_RETRY_INTERVAL_MS, keyNum, 10)

    // 每个触发间隔处理的最大偏移量的速率限制，指定的偏移总数将在不同卷的topicPartitions中按比例分配
    def kafkaMaxOffsetsPerTrigger(keyNum: Int = 1): Long = PropUtils.getLong(PropKeys.KAFKA_MAX_OFFSETS_PER_TRIGGER, keyNum, -1)

    // 丢失数据时是否失败
    def kafkaFailOnDataLoss(keyNum: Int = 1): Boolean = PropUtils.getBoolean(PropKeys.KAFKA_FAIL_ON_DATA_LOSS, keyNum, DefaultVals.kafkaFailOnDataLoss)

    // enable.auto.commit
    def kafkaEnableAutoCommit(keyNum: Int = 1): Boolean = PropUtils.getBoolean(PropKeys.KAFKA_ENABLE_AUTO_COMMIT, keyNum, DefaultVals.kafkaEnableAutoCommit)

    /**
     * 配置文件中的groupId
     *
     * @param keyNum
     * 序列
     * @return
     * 配置信息
     */
    def kafkaGroupId(keyNum: Int = 1): String = PropUtils.getString(PropKeys.KAFKA_GROUP_ID, keyNum, "")

    /**
     * 根据名称获取kafka broker地址
     *
     * @param keyNum
     * 序列
     * @return
     * 配置信息
     */
    def kafkaBrokers(keyNum: Int = 1): String = {
      val brokerName = PropUtils.getString(PropKeys.KAFKA_BROKERS_NAME, keyNum, DefaultVals.kafkaBrokersName)
      val kafkaAddress = if (StringUtils.isNotBlank(brokerName) && brokerName.contains(":")) {
        brokerName
      } else if (this.kafkaMap.contains(brokerName)) {
        this.kafkaMap.get(brokerName).get
      } else {
        throw new IllegalArgumentException(s"未找到匹配的kafka地址，请检查参数：spark.kafka.brokers.name$keyNum")
      }
      kafkaAddress
    }

    /**
     * 获取topic列表
     *
     * @param keyNum
     * @return
     */
    def kafkaTopics(keyNum: Int = 1): String = {
      val topics = PropUtils.getString(PropKeys.KAFKA_TOPICS, keyNum, null)
      ValueUtils.requireNonNullForce(topics, "配置未找到：spark.kafka.topics" + keyNum)
      topics
    }

    /**
     * kafka session超时时间，默认5分钟
     *
     * @param keyNum
     * 配置的key后缀
     * @return
     */
    def kafkaSessionTimeOut(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_SESSION_TIMEOUT_MS, keyNum, 300000)
    }

    /**
     * kafka request超时时间
     *
     * @param keyNum
     * 配置的key后缀
     * @return
     */
    def kafkaRequestTimeOut(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_REQUEST_TIMEOUT_MS, keyNum, 400000)
    }

    /**
     * kafka request超时时间，默认10分钟
     *
     * @param keyNum
     * 配置的key后缀
     * @return
     */
    def kafkaPollInterval(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_MAX_POLL_INTERVAL_MS, keyNum, 600000)
    }

    /**
     * 心跳间隔时间：heartbeat.interval.ms
     *
     * @param keyNum
     * 配置的key后缀
     * @return
     */
    def kafkaHeartbeatInterval(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_HEARTBEAT_INTERVAL_MS, keyNum, -1)
    }

    /**
     * 消费者组最大的session超时时间：group.max.session.timeout.ms
     *
     * @param keyNum
     * 配置的key后缀
     * @return
     */
    def kafkaGroupMaxSessionTimeOut(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS, keyNum, -1)
    }

    /**
     * 消费者组最小的session超时时间：group.min.session.timeout.ms
     *
     * @param keyNum
     * 配置的key后缀
     * @return
     */
    def kafkaGroupMinSessionTimeOut(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS, keyNum, -1)
    }

    /**
     * 一次调用pool返回的最大记录数：max.poll.records
     *
     * @param keyNum
     * 配置的key后缀
     * @return
     */
    def kafkaMaxPollRecords(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_MAX_POLL_RECORDS, keyNum, -1)
    }

    /**
     * 每个分区返回的最大数据量：max.partition.fetch.bytes
     *
     * @param keyNum
     * 配置的key后缀
     * @return
     */
    def kafkaMaxPartitionFetchBytes(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_MAX_PARTITION_FETCH_BYTES, keyNum, -1)
    }
  }

  /**
   * rocketMQ相关配置
   */
  object RocketConf extends Enumeration {
    val rocketOffsetLargest = "latest"
    val rocketOffsetSmallest = "earliest"
    val rocketConsumerTag = "*"

    /**
     * 获取消费位点
     *
     * @return
     */
    def rocketStartingOffset(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_STARTING_OFFSET, keyNum, DefaultVals.rocketStartingOffset)
    }

    // 丢失数据时是否失败
    def rocketFailOnDataLoss(keyNum: Int = 1): Boolean = PropUtils.getBoolean(PropKeys.ROCKET_FAIL_ON_DATA_LOSS, keyNum, DefaultVals.rocketFailOnDataLoss)

    // enable.auto.commit
    def rocketEnableAutoCommit(keyNum: Int = 1): Boolean = PropUtils.getBoolean(PropKeys.ROCKET_ENABLE_AUTO_COMMIT, keyNum, DefaultVals.rocketEnableAutoCommit)

    /**
     * 获取rocketMQ name server 地址
     *
     * @param keyNum
     * 序列
     * @return
     * 配置信息
     */
    def rocketNameServer(keyNum: Int = 1): String = {
      val brokerName = PropUtils.getString(PropKeys.ROCKET_BROKERS_NAME, keyNum, "")
      ValueUtils.requireNonNullForce(brokerName, "配置未找到：spark.rocket.brokers.name" + keyNum)
      brokerName
    }

    /**
     * 获取rocketMQ 订阅的tag
     *
     * @param keyNum
     * 序列
     * @return
     * 配置信息
     */
    def rocketConsumerTag(keyNum: Int = 1): String = PropUtils.getString(PropKeys.ROCKET_CONSUMER_TAG, keyNum, "*")

    /**
     * 获取groupId
     *
     * @param keyNum
     * 序列
     * @return
     * 配置信息
     */
    def rocketGroupId(keyNum: Int = 1): String = {
      val groupId = PropUtils.getString(PropKeys.ROCKET_GROUP_ID, keyNum, "")
      ValueUtils.requireNonNullForce(groupId, "配置未找到：spark.rocket.group.id" + keyNum)
      groupId
    }

    /**
     * 获取rocket topic列表
     *
     * @param keyNum
     * 序列
     * @return
     * 配置信息
     */
    def rocketTopics(keyNum: Int = 1): String = {
      val topics = PropUtils.getString(PropKeys.ROCKET_TOPICS, keyNum, null)
      ValueUtils.requireNonNullForce(topics, "配置未找到：spark.rocket.topics" + keyNum)
      topics
    }

    def rocketNameserverPollInterval(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_NAMESERVER_POLL_INTERVAL, keyNum, "")
    }

    def rocketBrokerserverHeartbeatInterval(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_BROKERSERVER_HEARTBEAT_INTERVAL, keyNum, "")
    }

    def rocketConsumerOffsetPersistInterval(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_CONSUMER_OFFSET_PERSIST_INTERVAL, keyNum, "")
    }

    def rocketConsumerMinThreads(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_CONSUMER_MIN_THREADS, keyNum, "")
    }

    def rocketConsumerMaxThreads(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_CONSUMER_MAX_THREADS, keyNum, "")
    }

    def rocketSpoutMessagesMaxRetry(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_SPOUT_MESSAGES_MAX_RETRY, keyNum, "")
    }

    def rocketPullMaxSpeedPerPartition(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_PULL_MAX_SPEED_PER_PARTITION, keyNum, "")
    }

    def rocketPullMaxBatchSize(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_PULL_MAX_BATCH_SIZE, keyNum, "")
    }

    def rocketPullTimeoutMs(keyNum: Int = 1): String = {
      PropUtils.getString(PropKeys.ROCKET_PULL_TIMEOUT_MS, keyNum, "")
    }
  }

  val hbaseDurability = if (StringUtils.isBlank(PropUtils.getString(PropKeys.HbaseDurability_KEY))) Durability.USE_DEFAULT
  else {
    val durability = PropUtils.getString(PropKeys.HbaseDurability_KEY)
    if ("ASYNC_WAL".equalsIgnoreCase(durability)) {
      Durability.ASYNC_WAL
    } else if ("FSYNC_WAL".equalsIgnoreCase(durability)) {
      Durability.FSYNC_WAL
    } else if ("SKIP_WAL".equalsIgnoreCase(durability)) {
      Durability.SKIP_WAL
    } else if ("SYNC_WAL".equalsIgnoreCase(durability)) {
      Durability.SYNC_WAL
    } else {
      Durability.USE_DEFAULT
    }
  }

  /**
   * hbase相关配置
   */
  object HBaseConf extends Enumeration {
    // HBase操作默认的批次大小
    lazy val hbaseBatchSize = PropUtils.getInt(PropKeys.HBASE_BATCH, DefaultVals.hbaseBatch)
  }

  /**
   * impala相关配置
   */
  object KuduConf extends Enumeration {
    val kuduMaster = PropUtils.getString(PropKeys.KUDU_MASTER_URL)
    val impalaConnectionUrl: String = PropUtils.getString(PropKeys.IMPALA_CONNECTION_URL_KEY)
    val impalaJdbcDriverName: String = PropUtils.getString(PropKeys.IMPALA_JDBC_DRIVER_NAME_KEY)
    val impalaDaemons: String = PropUtils.getString(PropKeys.IMPALA_DAEMONS_URL, "")
  }


  /**
   * 周期相关字符串
   */
  object Cron extends Enumeration {
    val HOUR = "hour"
    val DAY = "day"
    val WEEK = "week"
    val MONTH = "month"
    val YEAR = "year"
    val MINUTE = "minute"
    val SECOND = "second"
    val enumSet = Set(HOUR, DAY, WEEK, MONTH, YEAR, MINUTE, SECOND)
  }

  /**
   * 颜色预定义
   */
  object PS1 extends Enumeration {
    // 颜色
    val GREEN = "\u001B[32m"
    val DEFAULT = "\u001B[0m"
    val RED = "\u001B[31m"
    val YELLOW = "\u001B[33m"
    val BLUE = "\u001B[34m"
    val PURPLE = "\u001B[35m"
    val PINK = "\u001B[35m"

    // 字体
    val HIGH_LIGHT = "\u001B[1m"
    val ITALIC = "\u001B[3m"
    val UNDER_LINE = "\u001B[4m"
    val FLICKER = "\u001B[5m"

    /**
     * 包裹处理
     *
     * @param str
     * 原字符串
     * @param ps1
     * ps1
     * @return
     * wrap后的字符串
     */
    def wrap(str: String, ps1: String*): String = {
      val printStr = new StringBuilder()
      ps1.foreach(ps => {
        printStr.append(ps)
      })
      printStr.append(str + DEFAULT).toString()
    }
  }

  /**
   * 日期模式类型
   */
  object DateTimeSchema extends Enumeration {
    val yyyy_MM_ddHHmmss = "yyyy-MM-dd HH:mm:ss"
    val yyyyMMdd = "yyyyMMdd"
    val yyyy_MM_dd = "yyyy-MM-dd"
    val yyyyMMddHH = "yyyyMMddHH"
  }

  /**
   * 打印模块枚举
   */
  object PrintModule extends Enumeration {
    // 打印多值累加器开始
    def MULTI_ACC_START = println(s"[${GlobalConstants.PS1.PINK}${DateFormatUtils.formatCurrentDateTime()}${GlobalConstants.PS1.DEFAULT}]--- ${GlobalConstants.PS1.GREEN}MultiAccumulators Start ... ${GlobalConstants.PS1.DEFAULT}---------------------------------------------")

    // 打印多值多日期累加器开始
    def MULTI_ACC_DATE_TIME_START = println(s"[${GlobalConstants.PS1.PINK}${DateFormatUtils.formatCurrentDateTime()}${GlobalConstants.PS1.DEFAULT}]--- ${GlobalConstants.PS1.GREEN}MultiDateTimeAccumulators Start ... ${GlobalConstants.PS1.DEFAULT}---------------------------------------------")

    // 打印多值累加器结束
    def MULTI_ACC_END = println(s"------------------------ ${GlobalConstants.PS1.GREEN}MultiAccumulators End   ... ${GlobalConstants.PS1.DEFAULT}---------------------------------------------\n\n")

    // 打印多值多日期累加器结束
    def MULTI_ACC_DATE_TIME_END = println(s"------------------------ ${GlobalConstants.PS1.GREEN}MultiDateTimeAccumulators End   ... ${GlobalConstants.PS1.DEFAULT}---------------------------------------------\n\n")

    // 打印多值累加器清零
    def MULTI_ACC_CLEAR = println(s"------------------------ ${GlobalConstants.PS1.RED}*********** 清零累加器 ***********${GlobalConstants.PS1.DEFAULT}  ---------------------------------------------")

    // 打印多值累加器中的值
    def MULTI_ACC_VALUE(t: (String, Long)) = println(s"${t._1} : ${GlobalConstants.PS1.YELLOW}${t._2}${GlobalConstants.PS1.DEFAULT}")

    // 总耗时打印
    def END_TIME_COST(startTime: Long) = println(s"总耗时：${GlobalConstants.PS1.RED}${DateFormatUtils.runTime(startTime)}${GlobalConstants.PS1.DEFAULT} The end...${GlobalConstants.PS1.DEFAULT}")

    // 实时相关
    def REAL_TIME_PROCESS_METHOD = s"${GlobalConstants.PS1.RED}子类必须通过覆写process()方法实现具体逻辑${GlobalConstants.PS1.DEFAULT}"
  }

  /**
   * 常量字符串
   */
  object Strings extends Enumeration {
    // 集群hostname前缀
    val hostNamePrefix = "HZPL025"
    // 集群ip前缀
    val ipPrefxi = "192.168.25."
  }

  /**
   * log相关常量
   */
  object LogVal extends Enumeration {
    // log info级别开始
    val logInfoSplitStart = "--->[ "
    // log info级别结束
    val logInfoSplitEnd = " ]<---"
    // log error级别开始
    val logErrorSplitStart = "===>[ "
    // log error级别结束
    val logErrorSplitEnd = " ]<==="
    val logStart = "<================================>"
    val logEnd = "<================================>\n"
  }

  /**
   * 预定义的一些正则表达式
   */
  object Regulars extends Enumeration {
    val DOUBLE_DATE_PATTERN = "\\d+_\\d+".r
    // 匹配形如2018051912的时间，前面有_
    val DATE_TIME_PATTERN = "_\\d{10}$".r
    // 匹配一个以上的数字
    val MULTI_NUMBER_PATTERN = "_\\d+$".r
    // 只能包含字母和下划线
    val NO_NUMBER = "^[A-Za-z_]+$".r
    // 匹配applicationId，兼容后缀为4位或5位数字
    val APPLICATION_ID = "application_\\d{13}_\\d{4,5}".r
  }

  /**
   * 日志的级别
   */
  object LogLevel extends Enumeration {
    val OFF = "OFF"
    val FATAL = "FATAL"
    val ERROR = "ERROR"
    val WARN = "WARN"
    val INFO = "INFO"
    val DEBUG = "DEBUG"
    val TRACE = "TRACE"
    val ALL = "ALL"
  }

  /**
   * hive相关配置
   */
  object HiveConf extends Enumeration {
    // 是否启用hive支持
    lazy val hiveSupportEnable = PropUtils.getBoolean(PropKeys.HIVE_SUPPORT_ENABLE, true)
    // hive集群标识（batch/streaming/test）
    lazy val hiveCluster = PropUtils.getString(PropKeys.HIVE_CLUSTER, DefaultVals.hiveCluster)
    // 初始化hive集群名称与metastore映射
    private lazy val hiveMetastoreMap = PropUtils.sliceKeys("spark.hive.cluster.map.")
    // hive-site.xml存放路径映射
    private lazy val hiveSiteMap = PropUtils.sliceKeys("spark.hive.site.path.map.")
    // hive版本号
    lazy val hiveVersion = PropUtils.getString(PropKeys.HIVE_VERSION, "1.1.0")
    // hive catalog名称
    lazy val hiveCatalogName = PropUtils.getString(PropKeys.HIVE_CATALOG_NAME, "hive")

    /**
     * 根据hive集群名称获取metastore地址
     *
     * @return
     * uri
     */
    def getMetastoreUrl: String = {
      val metastore = if (StringUtils.isNotBlank(hiveCluster) && hiveCluster.contains(":")) {
        hiveCluster
      } else if (this.hiveMetastoreMap.contains(hiveCluster)) {
        this.hiveMetastoreMap.get(hiveCluster).get
      } else {
        throw new IllegalArgumentException(s"未找到匹配的hive metastore地址，请检查参数：spark.hive.cluster")
      }
      metastore
    }

    /**
     * 获取hive-site.xml的存放路径
     *
     * @return
     * /path/to/hive-site.xml
     */
    def getHiveConfDir: String = {
      val hiveSitePath = if (StringUtils.isNotBlank(hiveCluster) && hiveCluster.contains("""/""")) {
        hiveCluster
      } else if (this.hiveSiteMap.contains(hiveCluster)) {
        this.hiveSiteMap.get(hiveCluster).get
      } else {
        throw new IllegalArgumentException(s"未找到匹配的hive-site.xml存放路径，请检查参数：spark.hive.cluster")
      }
      hiveSitePath
    }
  }

  /**
   * 预设状态
   */
  object Status extends Enumeration {
    val SUCCESS = "SUCCESS"
    val FAILED = "FAILED"
    val ERROR = "ERROR"
    val FINISHED = "FINISHED"
    val RUNNING = "RUNNING"
    val UNKNOWN = "UNKNOWN"
  }

  /**
   * 用于定义累加日期的维度
   */
  object MultiTimerSchema extends Enumeration {
    val SEC = "yyyy-MM-dd HH:mm:ss"
    val MIN = "yyyy-MM-dd HH:mm:00"
    val HOUR = "yyyy-MM-dd HH:00:00"
    val DAY = "yyyy-MM-dd 00:00:00"

    /**
     * 其他用于自定义日期格式
     */
    def other(schema: String): String = schema
  }

  /**
   * HDFS配置
   */
  object HdfsConf extends Enumeration {
    // 配置是否启用hdfs HA
    lazy val hdfsHAEnable = PropUtils.getBoolean(PropKeys.HDFS_HA, DefaultVals.enableHdfsHA)

    /**
     * hdfs高可用关联hive集群
     */
    def linkHiveCluster(hadoopConf: Configuration): Unit = {
      if (hadoopConf != null && this.hdfsHAEnable) {
        val hdfsHAConf = PropUtils.sliceKeys(s"spark.hdfs.ha.fire_conf.${HiveConf.hiveCluster}.")
        hdfsHAConf.foreach(kv => {
          if (StringUtils.isBlank(kv._2)) throw new IllegalArgumentException(s"hdfs HA参数不合法，请检查配置项：${kv._1}")
          hadoopConf.set(kv._1, kv._2)
        })
      }
    }
  }

}