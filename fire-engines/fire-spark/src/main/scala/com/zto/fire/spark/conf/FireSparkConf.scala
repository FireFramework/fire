package com.zto.fire.spark.conf

import com.zto.fire.common.util.PropUtils

/**
 * Spark引擎相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:57
 */
private[fire] object FireSparkConf {
  lazy val APP_NAME_KEY = "spark.appName"
  lazy val SPARK_CONF_KEY = "SparkConf"
  lazy val SPARK_LOCAL_CORES = "spark.local.cores"
  lazy val LOG_LEVEL = "spark.log.level"
  lazy val SAVE_MODE_KEY = "spark.saveMode"
  lazy val PARALLELISM_KEY = "spark.parallelism"
  lazy val SPARK_CHK_POINT_DIR = "spark.chkpoint.dir"
  // spark 默认的checkpoint地址
  lazy val sparkChkPointDir = "hdfs://nameservice1/user/spark/ckpoint/"
  // spark streaming批次时间
  lazy val SPARK_STREAMING_BATCH_DURATION = "spark.streaming.batch.duration"

  lazy val appName = PropUtils.getString(this.APP_NAME_KEY, "")
  lazy val localCores = PropUtils.getString(this.SPARK_LOCAL_CORES, "*")
  lazy val sparkConf = PropUtils.getString(this.SPARK_CONF_KEY)
  lazy val logLevel = PropUtils.getString(this.LOG_LEVEL, "info").toUpperCase
  lazy val saveMode = PropUtils.getString(this.SAVE_MODE_KEY, "Append")
  lazy val parallelism = PropUtils.getInt(this.PARALLELISM_KEY)
  lazy val chkPointDirPrefix = PropUtils.getString(this.SPARK_CHK_POINT_DIR, this.sparkChkPointDir)
  lazy val confBathDuration = PropUtils.getInt(this.SPARK_STREAMING_BATCH_DURATION, -1)
}
