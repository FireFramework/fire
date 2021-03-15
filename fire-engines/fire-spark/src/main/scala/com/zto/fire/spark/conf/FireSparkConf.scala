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
  lazy val SPARK_APP_NAME = "spark.appName"
  lazy val SPARK_LOCAL_CORES = "spark.local.cores"
  lazy val SPARK_LOG_LEVEL = "spark.log.level"
  lazy val SPARK_SAVE_MODE = "spark.saveMode"
  lazy val SPARK_PARALLELISM = "spark.parallelism"
  lazy val SPARK_CHK_POINT_DIR = "spark.chkpoint.dir"
  // spark 默认的checkpoint地址
  lazy val sparkChkPointDir = "hdfs://nameservice1/user/spark/ckpoint/"
  // spark streaming批次时间
  lazy val SPARK_STREAMING_BATCH_DURATION = "spark.streaming.batch.duration"
  // spark streaming的remember时间，-1表示不生效(ms)
  lazy val SPARK_STREAMING_REMEMBER = "spark.streaming.remember"

  // spark streaming的remember时间，-1表示不生效(ms)
  def streamingRemember: Long = PropUtils.getLong(this.SPARK_STREAMING_REMEMBER, -1)
  lazy val appName = PropUtils.getString(this.SPARK_APP_NAME, "")
  lazy val localCores = PropUtils.getString(this.SPARK_LOCAL_CORES, "*")
  lazy val logLevel = PropUtils.getString(this.SPARK_LOG_LEVEL, "info").toUpperCase
  lazy val saveMode = PropUtils.getString(this.SPARK_SAVE_MODE, "Append")
  lazy val parallelism = PropUtils.getInt(this.SPARK_PARALLELISM)
  lazy val chkPointDirPrefix = PropUtils.getString(this.SPARK_CHK_POINT_DIR, this.sparkChkPointDir)
  lazy val confBathDuration = PropUtils.getInt(this.SPARK_STREAMING_BATCH_DURATION, -1)
}
