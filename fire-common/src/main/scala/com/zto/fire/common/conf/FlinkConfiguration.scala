package com.zto.fire.common.conf

import com.zto.fire.common.util.{GlobalConstants, PropUtils}

/**
 * flink相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:55
 */
class FlinkConfiguration extends Enumeration {
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
