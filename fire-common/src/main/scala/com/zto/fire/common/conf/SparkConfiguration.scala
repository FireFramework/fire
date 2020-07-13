package com.zto.fire.common.conf

import com.zto.fire.common.util.GlobalConstants.{DefaultVals, PropKeys}
import com.zto.fire.common.util.PropUtils

/**
 * Spark引擎相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:57
 */
class SparkConfiguration extends Enumeration {
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