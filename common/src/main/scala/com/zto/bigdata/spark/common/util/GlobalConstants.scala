package com.zto.bigdata.spark.common.util

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Durability
import org.apache.spark.sql.SaveMode

/**
  * 常量配置类
  * Created by ChengLong on 2016-11-22.
  */
object GlobalConstants {

  /**
    * 预定义的默认值，配置文件没有指明的情况下会取默认值
    */
  private[this] object DefaultVals extends Enumeration {
    val clusterName = "batch"
    // 默认的kafka broker地址
    val kafkaBrokers = "192.168.11.101:9092,192.168.11.102:9092,192.168.11.103:9092"
    // 默认的zookeeper地址
    val zkUrl = "192.168.25.38:2181,192.168.25.39:2181,192.168.25.40:2181,192.168.25.41:2181,192.168.25.42:2181"
    // spark 默认的checkpoint地址
    val sparkChkPointDir = "hdfs://nameservice1/user/spark/ckpoint/"
    // hive metastore地址
    val hiveMetaStoreUris = "thrift://192.168.25.36:9083"
  }

  /**
    * 对应conf.properties的key
    */
  private[this] object PropKeys extends Enumeration {
    // 运行模式
    val RUNMODEL_KEY = "runModel"
    val APP_NAME_KEY = "appName"
    val SPARK_CONF_KEY = "SparkConf"
    // c3p0连接池相关配置
    val URL_KEY = "jdbcUrl"
    val DRIVER_CLASS_KEY = "driverClass"
    val USER_KEY = "user"
    val PASSWORD_KEY = "password"
    val MAX_POOL_SIZE_KEY = "maxPoolSize"
    val MIN_POOL_SIZE_KEY = "minPoolSize"
    val ACQUIRE_INCREMENT_KEY = "acquireIncrement"
    val INITIAL_POOL_SIZE_KEY = "initialPoolSize"
    val MAX_IDLE_TIME_KEY = "maxIdleTime"
    val LOG_LEVEL = "LogLevel"
    val SAVE_MODE_KEY = "saveMode"
    val PARALLELISM_KEY = "parallelism"
    val FAMILY_KEY = "family"
    val HbaseDurability_KEY = "HbaseDurability"
    val KUDU_MASTER_URL = "kudu.master"
    val CLUSERT_NAME_URL = "clusert.name"
    val ZK_URL = "zk.url"
    val IMPALA_CONNECTION_URL_KEY: String = "impala.connection.url"
    val IMPALA_JDBC_DRIVER_NAME_KEY: String = "impala.jdbc.driver.class.name"
    val IMPALA_DAEMONS_URL = "impala.daemons.url"
    val KAFKA_BROKERS_URL = "kafka.brokers.url"

    // spark相关配置
    val SPARK_CHK_POINT_DIR = "spark.chkpoint.dir"

    // carbondata相关配置
    val CARBON_STORE_PATH = "carbon.storePath"
    val CARBON_META_STORE_PATH = "carbon.metaStorePath"

    // hive相关配置
    val HIVE_METASTORE_URIS = "hive.metastore.uris"
  }

  /**
    * 关系型数据库相关配置
    */
  // object RDBMSConf extends Enumeration {
  val rdburl = PropUtils.getString(PropKeys.URL_KEY)
  val driverClass = PropUtils.getString(PropKeys.DRIVER_CLASS_KEY)
  val user = PropUtils.getString(PropKeys.USER_KEY)
  val password = PropUtils.getString(PropKeys.PASSWORD_KEY)
  val maxPoolSize = PropUtils.getInt(PropKeys.MAX_POOL_SIZE_KEY)
  val minPoolSize = PropUtils.getInt(PropKeys.MIN_POOL_SIZE_KEY)
  val acquireIncrement = PropUtils.getInt(PropKeys.ACQUIRE_INCREMENT_KEY)
  val initialPoolSize = PropUtils.getInt(PropKeys.INITIAL_POOL_SIZE_KEY)
  val maxIdleTime = PropUtils.getInt(PropKeys.MAX_IDLE_TIME_KEY)

  // }

  /**
    * 集群相关配置
    */
  // object ClusterConf extends Enumeration {
  val CLUSTER_KEY = "cluster"
  val CLUSERT_NAME = PropUtils.getString(PropKeys.CLUSERT_NAME_URL, DefaultVals.clusterName)
  val isCluster = if (CLUSTER_KEY.equalsIgnoreCase(PropUtils.getString(PropKeys.RUNMODEL_KEY))) true else false
  val isLocal = !isCluster
  // spark运行时日志记录表
  val sparkRuntimeLogTable = "spark_runtime_log"
  // kafka broker地址
  val kafkaBrokers = PropUtils.getString(PropKeys.KAFKA_BROKERS_URL, DefaultVals.kafkaBrokers)
  // zookeeper地址
  val zkUrl = PropUtils.getString(PropKeys.ZK_URL, DefaultVals.zkUrl)

  // }

  /**
    * Spark相关常量配置
    */
  object SparkConf extends Enumeration {
    val appName = PropUtils.getString(PropKeys.APP_NAME_KEY, "spark")
    val sparkConf = PropUtils.getString(PropKeys.SPARK_CONF_KEY)
    val logLevel = PropUtils.getString(PropKeys.LOG_LEVEL)
    val saveMode = if ("Overwrite".equalsIgnoreCase(PropUtils.getString(PropKeys.SAVE_MODE_KEY))) SaveMode.Overwrite else SaveMode.Append
    val parallelism = PropUtils.getInt(PropKeys.PARALLELISM_KEY)
    val CHK_POINT_DIR_PREFIX = PropUtils.getString(PropKeys.SPARK_CHK_POINT_DIR, DefaultVals.sparkChkPointDir)
  }

  /**
    * kafka相关配置
    */
  object KafkaConf extends Enumeration {
    val offsetLargest = "latest"
    val offsetSmallest = "earliest"
    val offsetNone = "none"
  }

  /**
    * hbase相关配置
    */
  // object HBaseConf extends Enumeration {
  val familyName = PropUtils.getString(PropKeys.FAMILY_KEY, "info") // hbase默认的列族名称，如果使用FieldName指定，则会被覆盖

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

  // }

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
  object Color extends Enumeration {
    val GREEN = "\u001B[32m"
    val DEFAULT = "\u001B[0m"
    val RED = "\u001B[31m"
    val YELLOW = "\u001B[33m"
    val BLUE = "\u001B[34m"
    val PINK = "\u001B[35m"
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
    def MULTI_ACC_START = println(s"[${GlobalConstants.Color.PINK}${DateFormatUtils.formatCurrentDateTime()}${GlobalConstants.Color.DEFAULT}]--- ${GlobalConstants.Color.GREEN}MultiAccumulators Start ... ${GlobalConstants.Color.DEFAULT}---------------------------------------------")
    // 打印多值多日期累加器开始
    def MULTI_ACC_DATE_TIME_START = println(s"[${GlobalConstants.Color.PINK}${DateFormatUtils.formatCurrentDateTime()}${GlobalConstants.Color.DEFAULT}]--- ${GlobalConstants.Color.GREEN}MultiDateTimeAccumulators Start ... ${GlobalConstants.Color.DEFAULT}---------------------------------------------")
    // 打印多值累加器结束
    def MULTI_ACC_END = println(s"------------------------ ${GlobalConstants.Color.GREEN}MultiAccumulators End   ... ${GlobalConstants.Color.DEFAULT}---------------------------------------------\n\n")
    // 打印多值多日期累加器结束
    def MULTI_ACC_DATE_TIME_END = println(s"------------------------ ${GlobalConstants.Color.GREEN}MultiDateTimeAccumulators End   ... ${GlobalConstants.Color.DEFAULT}---------------------------------------------\n\n")
    // 打印多值累加器清零
    def MULTI_ACC_CLEAR = println(s"------------------------ ${GlobalConstants.Color.RED}*********** 清零累加器 ***********${GlobalConstants.Color.DEFAULT}  ---------------------------------------------")
    // 打印多值累加器中的值
    def MULTI_ACC_VALUE(t: (String, Long)) = println(s"${t._1} : ${GlobalConstants.Color.YELLOW}${t._2}${GlobalConstants.Color.DEFAULT}")
    // 总耗时打印
    def END_TIME_COST(startTime: Long) = println(s"总耗时：${GlobalConstants.Color.RED}${SparkUtils.runTime(startTime)}${GlobalConstants.Color.DEFAULT} The end...${GlobalConstants.Color.DEFAULT}")
    // 实时相关
    def REAL_TIME_PROCESS_METHOD = s"${GlobalConstants.Color.RED}子类必须通过覆写process()方法实现具体逻辑${GlobalConstants.Color.DEFAULT}"
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
  }

  /**
    * carbondata相关配置
    */
  object CarbonConf extends Enumeration {
    val storePath = PropUtils.getString(PropKeys.CARBON_STORE_PATH)
    val metaStorePath = PropUtils.getString(PropKeys.CARBON_META_STORE_PATH)
  }

  /**
    * hive相关配置
    */
  object HiveConf extends Enumeration {
    // hive metastore地址
    val metaStoreUris = PropUtils.getString(PropKeys.HIVE_METASTORE_URIS, DefaultVals.hiveMetaStoreUris)
  }

}