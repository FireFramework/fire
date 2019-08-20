package com.zto.fire.common.util

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Durability
import org.apache.rocketmq.spark.ConsumerStrategy
import org.apache.spark.sql.SaveMode

/**
  * 常量配置类
  * Created by ChengLong on 2016-11-22.
  */
object GlobalConstants {

  /**
    * 预定义的默认值，配置文件没有指明的情况下会取默认值
    */
  object DefaultVals extends Enumeration {
    // hbase集群名，用于区分不同的hbase-site.xml文件
    val hbaseName = "batch"

    // rest接口filter的开关
    val restFilter = true

    // 默认的kafka broker地址
    val kafkaBrokers = "192.168.25.80:9092,192.168.25.81:9092,192.168.25.82:9092,192.168.25.129:9092,192.168.25.130:9092,192.168.25.131:9092"
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

    // 默认的zookeeper地址
    val zkUrl = "192.168.25.38:2181,192.168.25.39:2181,192.168.25.40:2181,192.168.25.41:2181,192.168.25.42:2181"
    // spark 默认的checkpoint地址
    val sparkChkPointDir = "hdfs://nameservice1/user/spark/ckpoint/"
    // hive metastore地址
    val hiveCluster = "streaming"
    // 默认的日志级别
    val logLevel = LogLevel.INFO
    // 累加器保留日志默认的最大记录数
    val maxLogSize = 1000
    // 默认的数据库名称
    val dbName = "tmp"
    // 默认的partition名称
    val partitionName = "ds"
    // HBase默认批次大小
    val hbaseBatch = 10000
  }

  /**
    * 对应conf.properties的key
    */
  object PropKeys extends Enumeration {
    // 运行模式
    val RUNMODEL_KEY = "spark.runModel"
    val APP_NAME_KEY = "spark.appName"
    val SPARK_CONF_KEY = "SparkConf"
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

    val LOG_LEVEL = "spark.log.level"
    val SAVE_MODE_KEY = "spark.saveMode"
    val PARALLELISM_KEY = "spark.parallelism"
    val HBBASE_COLUMN_FAMILY_KEY = "spark.hbase.column.family"
    val HbaseDurability_KEY = "HbaseDurability"
    val KUDU_MASTER_URL = "spark.kudu.master"
    val HBASE_CLUSTER_URL = "spark.hbase.cluster"
    val HBASE_BATCH = "spark.hbase.batch.size"
    val IMPALA_CONNECTION_URL_KEY: String = "spark.impala.connection.url"
    val IMPALA_JDBC_DRIVER_NAME_KEY: String = "spark.impala.jdbc.driver.class.name"
    val IMPALA_DAEMONS_URL = "spark.impala.daemons.url"
    val KAFKA_BROKERS_NAME = "spark.kafka.brokers.name"
    // kafka的topic列表，以逗号分隔
    val KAFKA_TOPICS = "spark.kafka.topics"
    // kafka起始消费位点
    val KAFKA_STARTING_OFFSET = "spark.kafka.starting.offsets"
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
    // int 1048576
    // 每个分区返回的最大数据量：max.partition.fetch.bytes
    val KAFKA_MAX_PARTITION_FETCH_BYTES = "spark.kafka.max.partition.fetch.bytes"

    // spark相关配置
    val SPARK_CHK_POINT_DIR = "spark.chkpoint.dir"
    val SPARK_LOG_LEVEL = "spark.log.level"
    // spark streaming批次时间
    val SPARK_STREAMING_BATCH_DURATION = "spark.streaming.batch.duration"

    // hive相关配置
    val HIVE_CLUSTER = "spark.hive.cluster"
    // 默认的库名
    val SPARK_DEFAULT_DATABASE_NAME = "spark.default.database.name"
    // 默认的分区名称
    val SPARK_DEFAULT_TABLE_PARTITION_NAME = "spark.default.table.partition.name"

    // rocketMQ相关配置
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
    // 日志记录器保留最大的记录数
    val SPARK_FIRE_LOG_MAX_SIZE = "spark.fire.log.max.size"
    // rest接口权限认证
    val SPARK_FIRE_REST_FILTER = "spark.fire.rest.filter"
  }

  /**
    * Fire框架相关配置
    */
  object FireConf extends Enumeration {
    // rest接口权限认证
    val restFilter = PropUtils.getBoolean(GlobalConstants.PropKeys.SPARK_FIRE_REST_FILTER, GlobalConstants.DefaultVals.restFilter)
  }

  /**
    * 关系型数据库连接池相关配置
    */
  object JdbcConf extends Enumeration {
    def url(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_URL_KEY, keyNum)

    def driverClass(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_DRIVER_KEY, keyNum)

    def user(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_USER_KEY, keyNum)

    def password(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_PASSWORD_KEY, keyNum)

    // 事务的隔离级别：NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, SERIALIZABLE，默认为READ_UNCOMMITTED
    def isolationLevel(keyNum: Int = 1): String = PropUtils.getString(PropKeys.SPARK_DB_JDBC_ISOLATION_LEVEL, keyNum, DefaultVals.jdbcIsolationLevel)

    // 批量操作的记录数
    def batchSize(keyNum: Int = 1): Int = PropUtils.getInt(PropKeys.SPARK_DB_JDBC_BATCH_SIZE, keyNum, DefaultVals.jdbcBatchSize)

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
    val sparkConf = PropUtils.getString(PropKeys.SPARK_CONF_KEY)
    val logLevel = PropUtils.getString(PropKeys.LOG_LEVEL, DefaultVals.logLevel)
    val saveMode = if ("Overwrite".equalsIgnoreCase(PropUtils.getString(PropKeys.SAVE_MODE_KEY))) SaveMode.Overwrite else SaveMode.Append
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

    // 大数据kafka地址
    private val bigdataKafkaUrl = "192.168.25.80:9092,192.168.25.81:9092,192.168.25.82:9092,192.168.25.129:9092,192.168.25.130:9092,192.168.25.131:9092"
    // zms kafka地址
    private val zmsKafkaUrl = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092,192.168.1.173:9092,192.168.5.29:9092,192.168.5.30:9092"
    // zms new 地址
    private val zmsNewKafkaUrl = "192.168.73.31:9092,192.168.73.32:9092,192.168.73.33:9092,192.168.73.34:9092,192.168.73.35:9092,192.168.73.36:9092"
    // 测试kafka集群地址
    private val testKafkaUrl = "10.9.45.97:9092,10.9.15.38:9092,10.9.36.49:9092,10.9.36.50:9092"

    // kafka消费位点
    def kafkaStartingOffset(keyNum: Int = 1): String = PropUtils.getString(PropKeys.KAFKA_STARTING_OFFSET, keyNum, DefaultVals.kafkaStartingOffset)

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
      if ("bigdata".equalsIgnoreCase(brokerName)) {
        bigdataKafkaUrl
      } else if ("zms".equalsIgnoreCase(brokerName)) {
        zmsKafkaUrl
      } else if ("zmsNew".equalsIgnoreCase(brokerName)) {
        zmsNewKafkaUrl
      } else if ("test".equalsIgnoreCase(brokerName)) {
        testKafkaUrl
      } else if (StringUtils.isNotBlank(brokerName) && brokerName.contains(":")) {
        brokerName
      } else {
        zmsKafkaUrl
      }
    }

    /**
      * 获取topic列表
      *
      * @param keyNum
      * @return
      */
    def kafkaTopics(keyNum: Int = 1): String = {
      val topics = PropUtils.getString(PropKeys.KAFKA_TOPICS, keyNum, null)
      ParamUtils.requireNonNullForce(topics, "配置未找到：spark.kafka.topics" + keyNum)
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
      PropUtils.getInt(PropKeys.KAFKA_HEARTBEAT_INTERVAL_MS, -1)
    }

    /**
      * 消费者组最大的session超时时间：group.max.session.timeout.ms
      *
      * @param keyNum
      * 配置的key后缀
      * @return
      */
    def kafkaGroupMaxSessionTimeOut(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS, -1)
    }

    /**
      * 消费者组最小的session超时时间：group.min.session.timeout.ms
      *
      * @param keyNum
      * 配置的key后缀
      * @return
      */
    def kafkaGroupMinSessionTimeOut(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS, -1)
    }

    /**
      * 一次调用pool返回的最大记录数：max.poll.records
      *
      * @param keyNum
      * 配置的key后缀
      * @return
      */
    def kafkaMaxPollRecords(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_MAX_POLL_RECORDS, -1)
    }

    /**
      * 每个分区返回的最大数据量：max.partition.fetch.bytes
      *
      * @param keyNum
      * 配置的key后缀
      * @return
      */
    def kafkaMaxPartitionFetchBytes(keyNum: Int = 1): java.lang.Integer = {
      PropUtils.getInt(PropKeys.KAFKA_MAX_PARTITION_FETCH_BYTES, -1)
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
    def rocketStartingOffset(keyNum: Int = 1): ConsumerStrategy = {
      val offset = PropUtils.getString(PropKeys.ROCKET_STARTING_OFFSET, keyNum, DefaultVals.rocketStartingOffset)
      if (rocketOffsetLargest.equalsIgnoreCase(offset)) {
        ConsumerStrategy.lastest
      } else {
        ConsumerStrategy.earliest
      }
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
      ParamUtils.requireNonNullForce(brokerName, "配置未找到：spark.rocket.brokers.name" + keyNum)
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
      ParamUtils.requireNonNullForce(groupId, "配置未找到：spark.rocket.group.id" + keyNum)
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
      ParamUtils.requireNonNullForce(topics, "配置未找到：spark.rocket.topics" + keyNum)
      topics
    }
  }

  /**
    * hbase相关配置
    */
  val familyName = PropUtils.getString(PropKeys.HBBASE_COLUMN_FAMILY_KEY, "info") // hbase默认的列族名称，如果使用FieldName指定，则会被覆盖

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
    val hbaseBatchSize = PropUtils.getInt(PropKeys.HBASE_BATCH, DefaultVals.hbaseBatch)
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
    // hive集群标识（batch/streaming/test）
    val hiveCluster = PropUtils.getString(PropKeys.HIVE_CLUSTER, DefaultVals.hiveCluster)
    // 离线hive集群
    private val batchMetastore = "thrift://192.168.25.36:9083"
    // 实时hive集群
    private val streamingMetastore = "thrift://192.168.25.180:9083"
    // 测试hive集群
    private val testMetastore = "thrift://10.9.46.107:9083"

    /**
      * 根据hive集群名称获取metastore地址
      *
      * @return
      * uri
      */
    def getMetastoreUrl: String = {
      if ("batch".equalsIgnoreCase(hiveCluster)) {
        batchMetastore
      } else if ("streaming".equalsIgnoreCase(hiveCluster)) {
        streamingMetastore
      } else if ("test".equalsIgnoreCase(hiveCluster)) {
        testMetastore
      } else if (StringUtils.isNotBlank(hiveCluster) && hiveCluster.contains(":")) {
        hiveCluster
      } else {
        streamingMetastore
      }
    }
  }

  // hbase集群名称
  lazy val hbaseCluster = PropUtils.getString(PropKeys.HBASE_CLUSTER_URL, DefaultVals.hbaseName)

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

}