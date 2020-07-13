package com.zto.fire.common.conf

/**
 * fire中支持的配置的key
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:51
 */
class PropKeysConfiguration extends Enumeration {
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
  val HBASE_DURABILITY = "spark.hbase.durability"
  val KUDU_MASTER_URL = "spark.kudu.master"
  val HBASE_CLUSTER_URL = "spark.hbase.cluster"
  val HBASE_BATCH = "spark.hbase.batch.size"
  val IMPALA_CONNECTION_URL_KEY = "spark.impala.connection.url"
  val IMPALA_JDBC_DRIVER_NAME_KEY = "spark.impala.jdbc.driver.class.name"
  val IMPALA_DAEMONS_URL = "spark.impala.daemons.url"

  // ---------------------------- kafka 相关配置 ---------------------------- //
  val KAFKA_BROKERS_NAME = "spark.kafka.brokers.name"
  // kafka的topic列表，以逗号分隔
  val KAFKA_TOPICS = "spark.kafka.topics"
  // group.id
  val KAFKA_GROUP_ID = "spark.kafka.group.id"
  // kafka起始消费位点
  val KAFKA_STARTING_OFFSET = "spark.kafka.starting.offsets"
  // kafka结束消费位点
  val KAFKA_ENDING_OFFSET = "spark.kafka.ending.offsets"
  // 是否自动维护offset
  val KAFKA_ENABLE_AUTO_COMMIT = "spark.kafka.enable.auto.commit"
  // 丢失数据是否失败
  val KAFKA_FAIL_ON_DATA_LOSS = "spark.kafka.failOnDataLoss"
  // kafka session超时时间
  val KAFKA_SESSION_TIMEOUT_MS = "spark.kafka.session.timeout.ms"
  // kafka request超时时间
  val KAFKA_REQUEST_TIMEOUT_MS = "spark.kafka.request.timeout.ms"
  val KAFKA_MAX_POLL_INTERVAL_MS = "spark.kafka.max.poll.interval.ms"
  val KAFKA_COMMIT_OFFSETS_ON_CHECKPOINTS = "spark.kafka.CommitOffsetsOnCheckpoints"
  val KAFKA_START_FROM_TIMESTAMP = "spark.kafka.StartFromTimestamp"
  val KAFKA_START_FROM_GROUP_OFFSETS = "spark.kafka.StartFromGroupOffsets"

  // ---------------------------- spark 相关配置 ---------------------------- //
  val SPARK_CHK_POINT_DIR = "spark.chkpoint.dir"
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
  val ROCKET_FORCE_SPECIAL = "spark.rocket.forceSpecial"
  // 是否自动维护offset
  val ROCKET_ENABLE_AUTO_COMMIT = "spark.rocket.enable.auto.commit"
  // RocketMQ起始消费位点
  val ROCKET_STARTING_OFFSET = "spark.rocket.starting.offsets"
  // rocketMq订阅的tag
  val ROCKET_CONSUMER_TAG = "spark.rocket.consumer.tag"
  // 每次拉取每个partition的消息数
  val ROCKET_PULL_MAX_SPEED_PER_PARTITION = "spark.rocket.pull.max.speed.per.partition"

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
  val SPARK_FIRE_HBASE_STORAGE_LEVEL = "spark.hbase.storage.level"
  // 通过HBase scan后repartition的分区数
  val SPARK_FIRE_HBASE_SCAN_REPARTITIONS = "spark.hbase.scan.repartitions"

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
  // zrc接口调用秘钥
  val SPARK_FIRE_ZRC_SECRET = "spark.zrc.register.conf.secret"
  // fire框架restful端口冲突重试次数
  val SPARK_FIRE_RESTFUL_PORT_RETRY_NUM = "spark.fire.restful.port.retry_num"
  // fire框架restful端口冲突重试时间（ms）
  val SPARK_FIRE_RESTFUL_PORT_RETRY_DURATION = "spark.fire.restful.port.retry_duration"
  val SPARK_LOG_LEVEL_CONF_PREFIX = "spark.log.level.fire_conf."

  // ---------------------------- HDFS 相关配置 ---------------------------- //
  // 是否启用高可用
  val HDFS_HA = "spark.hdfs.ha.enable"
  val HDFS_HA_PREFIX = "spark.hdfs.ha.conf."

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
