/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.flink.conf

import com.zto.fire.common.util.PropUtils

/**
 * flink相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 14:55
 */
private[fire] object FireFlinkConf {
  lazy val FLINK_AUTO_GENERATE_UID_ENABLE = "flink.auto.generate.uid.enable"
  lazy val FLINK_AUTO_TYPE_REGISTRATION_ENABLE = "flink.auto.type.registration.enable"
  lazy val FLINK_FORCE_AVRO_ENABLE = "flink.force.avro.enable"
  lazy val FLINK_FORCE_KRYO_ENABLE = "flink.force.kryo.enable"
  lazy val FLINK_GENERIC_TYPES_ENABLE = "flink.generic.types.enable"
  lazy val FLINK_OBJECT_REUSE_ENABLE = "flink.object.reuse.enable"
  lazy val FLINK_AUTO_WATERMARK_INTERVAL = "flink.auto.watermark.interval"
  lazy val FLINK_CLOSURE_CLEANER_LEVEL = "flink.closure.cleaner.level"
  lazy val FLINK_DEFAULT_INPUT_DEPENDENCY_CONSTRAINT = "flink.default.input.dependency.constraint"
  lazy val FLINK_EXECUTION_MODE = "flink.execution.mode"
  lazy val FLINK_RUNTIME_MODE = "flink.runtime.mode"
  lazy val FLINK_LATENCY_TRACKING_INTERVAL = "flink.latency.tracking.interval"
  lazy val FLINK_MAX_PARALLELISM = "flink.max.parallelism"
  lazy val FLINK_DEFAULT_PARALLELISM = "flink.default.parallelism"
  lazy val FLINK_TASK_CANCELLATION_INTERVAL = "flink.task.cancellation.interval"
  lazy val FLINK_TASK_CANCELLATION_TIMEOUT_MILLIS = "flink.task.cancellation.timeout.millis"
  lazy val FLINK_USE_SNAPSHOT_COMPRESSION = "flink.use.snapshot.compression"
  lazy val FLINK_STREAM_BUFFER_TIMEOUT_MILLIS = "flink.stream.buffer.timeout.millis"
  lazy val FLINK_STREAM_NUMBER_EXECUTION_RETRIES = "flink.stream.number.execution.retries"
  lazy val FLINK_STREAM_TIME_CHARACTERISTIC = "flink.stream.time.characteristic"
  lazy val FLINK_DRIVER_CLASS_NAME = "flink.driver.class.name"
  lazy val FLINK_CLIENT_SIMPLE_CLASS_NAME = "flink.client.simple.class.name"
  lazy val FLINK_SQL_CONF_UDF_JARS = "flink.sql.conf.pipeline.jars"
  lazy val FLINK_SQL_LOG_ENABLE = "flink.sql.log.enable"
  lazy val FLINK_SQL_DEFAULT_CATALOG_NAME = "flink.sql.default.catalog.name"
  lazy val FLINK_STATE_TTL_DAYS = "flink.state.ttl.days"
  lazy val DISTRIBUTE_SYNC_ENABLE = "fire.distribute.sync.enable"
  lazy val OPERATOR_CHAINING_ENABLE = "flink.env.operatorChaining.enable"

  // checkpoint相关配置项
  lazy val FLINK_STREAM_CHECKPOINT_INTERVAL = "flink.stream.checkpoint.interval"
  lazy val FLINK_STREAM_CHECKPOINT_MODE = "flink.stream.checkpoint.mode"
  lazy val FLINK_STREAM_CHECKPOINT_TIMEOUT = "flink.stream.checkpoint.timeout"
  lazy val FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT = "flink.stream.checkpoint.max.concurrent"
  lazy val FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN = "flink.stream.checkpoint.min.pause.between"
  lazy val FLINK_STREAM_CHECKPOINT_PREFER_RECOVERY = "flink.stream.checkpoint.prefer.recovery"
  lazy val FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER = "flink.stream.checkpoint.tolerable.failure.number"
  lazy val FLINK_STREAM_CHECKPOINT_EXTERNALIZED = "flink.stream.checkpoint.externalized"
  lazy val FLINK_STREAM_CHECKPOINT_UNALIGNED = "flink.stream.checkpoint.unaligned.enable"
  lazy val FLINK_SQL_WITH_REPLACE_MODE_ENABLE = "flink.sql_with.replaceMode.enable"
  lazy val FLINK_STATE_CLEAN_HDFS_URL = "flink.state.clean.hdfs.url"

  // udf自动注册
  lazy val FLINK_SQL_UDF_CONF_PREFIX = "flink.sql.udf.conf."
  lazy val FLINK_SQL_UDF_ENABLE = "flink.sql.udf.fireUdf.enable"

  /**
   * 获取所有flink.sql.with.为前缀的配置信息如：
   * flink.sql.with.bill_db.connector	=	mysql
   * flink.sql.with.bill_db.url			  =	jdbc:mysql://localhost:3306/fire
   * 上述配置标识定义名为bill_db的数据源，配置了两个options选项分别为：
   * connector	=	mysql
   * url			  =	jdbc:mysql://localhost:3306/fire
   * sql中即可通过 'datasource'='bill_db' 引用到上述两项option
   */
  lazy val FLINK_SQL_WITH_PREFIX = "flink.sql.with."
  lazy val FLINK_SQL_USE_STATEMENT_SET = "flink.sql.useStatementSet"

  lazy val defaultCatalogName = PropUtils.getString(this.FLINK_SQL_DEFAULT_CATALOG_NAME, "default_catalog")
  lazy val sqlWithReplaceModeEnable = PropUtils.getBoolean(this.FLINK_SQL_WITH_REPLACE_MODE_ENABLE, true)
  lazy val autoGenerateUidEnable = PropUtils.getBoolean(this.FLINK_AUTO_GENERATE_UID_ENABLE, true)
  lazy val autoTypeRegistrationEnable = PropUtils.getBoolean(this.FLINK_AUTO_TYPE_REGISTRATION_ENABLE, true)
  lazy val forceAvroEnable = PropUtils.getBoolean(this.FLINK_FORCE_AVRO_ENABLE, false)
  lazy val forceKryoEnable = PropUtils.getBoolean(this.FLINK_FORCE_KRYO_ENABLE, false)
  lazy val genericTypesEnable = PropUtils.getBoolean(this.FLINK_GENERIC_TYPES_ENABLE, true)
  lazy val objectReuseEnable = PropUtils.getBoolean(this.FLINK_OBJECT_REUSE_ENABLE, false)
  lazy val autoWatermarkInterval = PropUtils.getLong(this.FLINK_AUTO_WATERMARK_INTERVAL, -1)
  lazy val closureCleanerLevel = PropUtils.getString(this.FLINK_CLOSURE_CLEANER_LEVEL)
  lazy val defaultInputDependencyConstraint = PropUtils.getString(this.FLINK_DEFAULT_INPUT_DEPENDENCY_CONSTRAINT)
  lazy val executionMode = PropUtils.getString(this.FLINK_EXECUTION_MODE)
  lazy val latencyTrackingInterval = PropUtils.getLong(this.FLINK_LATENCY_TRACKING_INTERVAL, -1)
  lazy val maxParallelism = PropUtils.getInt(this.FLINK_MAX_PARALLELISM, 1024)
  lazy val defaultParallelism = PropUtils.getInt(this.FLINK_DEFAULT_PARALLELISM, -1)
  lazy val taskCancellationInterval = PropUtils.getLong(this.FLINK_TASK_CANCELLATION_INTERVAL, -1)
  lazy val taskCancellationTimeoutMillis = PropUtils.getLong(this.FLINK_TASK_CANCELLATION_TIMEOUT_MILLIS, -1)
  lazy val useSnapshotCompression = PropUtils.getBoolean(this.FLINK_USE_SNAPSHOT_COMPRESSION, false)
  lazy val streamBufferTimeoutMillis = PropUtils.getLong(this.FLINK_STREAM_BUFFER_TIMEOUT_MILLIS, -1)
  lazy val streamNumberExecutionRetries = PropUtils.getInt(this.FLINK_STREAM_NUMBER_EXECUTION_RETRIES, -1)
  lazy val streamTimeCharacteristic = PropUtils.getString(this.FLINK_STREAM_TIME_CHARACTERISTIC, "")
  lazy val sqlLogEnable = PropUtils.getBoolean(this.FLINK_SQL_LOG_ENABLE, false)
  lazy val unalignedCheckpointEnable = PropUtils.getBoolean(this.FLINK_STREAM_CHECKPOINT_UNALIGNED, true)
  lazy val distributeSyncEnabled = PropUtils.getBoolean(this.DISTRIBUTE_SYNC_ENABLE, true)

  // checkpoint相关配置项
  lazy val streamCheckpointInterval = PropUtils.getLong(this.FLINK_STREAM_CHECKPOINT_INTERVAL, -1)
  lazy val streamCheckpointMode = PropUtils.getString(this.FLINK_STREAM_CHECKPOINT_MODE, "EXACTLY_ONCE")
  lazy val streamCheckpointTimeout = PropUtils.getLong(this.FLINK_STREAM_CHECKPOINT_TIMEOUT, 600000L)
  lazy val streamCheckpointMaxConcurrent = PropUtils.getInt(this.FLINK_STREAM_CHECKPOINT_MAX_CONCURRENT, 1)
  lazy val streamCheckpointMinPauseBetween = PropUtils.getInt(this.FLINK_STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN, -1)
  lazy val streamCheckpointPreferRecovery = PropUtils.getBoolean(this.FLINK_STREAM_CHECKPOINT_PREFER_RECOVERY, false)
  lazy val streamCheckpointTolerableFailureNumber = PropUtils.getInt(this.FLINK_STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUMBER, 0)
  lazy val streamCheckpointExternalized = PropUtils.getString(this.FLINK_STREAM_CHECKPOINT_EXTERNALIZED, "RETAIN_ON_CANCELLATION")

  // 用于自动注册udf jar包中的函数
  lazy val flinkUdfList = PropUtils.sliceKeys(this.FLINK_SQL_UDF_CONF_PREFIX)
  // 是否启用fire udf注册功能
  lazy val flinkUdfEnable = PropUtils.getBoolean(this.FLINK_SQL_UDF_ENABLE, true)
  // 运行模式
  lazy val flinkRuntimeMode = PropUtils.getString(this.FLINK_RUNTIME_MODE, PropUtils.getString("execution.runtime-mode", "STREAMING"))
  // 默认的Keyed State的TTL时间
  lazy val flinkStateTTL = PropUtils.getInt(this.FLINK_STATE_TTL_DAYS, 31)
  // 是否开启算子链合并
  lazy val operatorChainingEnable = PropUtils.getBoolean(this.OPERATOR_CHAINING_ENABLE, true)
  // 是否自动将insert语句加入到StatementSet中
  lazy val autoAddStatementSet = PropUtils.getBoolean(this.FLINK_SQL_USE_STATEMENT_SET, true)
  // 将配置的with options映射为map
  lazy val flinkSqlWithOptions = PropUtils.sliceKeys(FireFlinkConf.FLINK_SQL_WITH_PREFIX)

  // flink状态清理的hdfs路径前缀
  lazy val stateHdfsUrl = PropUtils.getString(this.FLINK_STATE_CLEAN_HDFS_URL)
}
