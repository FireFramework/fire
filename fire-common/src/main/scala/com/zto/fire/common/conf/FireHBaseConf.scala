package com.zto.fire.common.conf

import java.util

import com.zto.fire.common.util.PropUtils

import scala.collection.JavaConversions

/**
 * hbase相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:08
 */
private[fire] object FireHBaseConf {
  lazy val HBASE_BATCH = "spark.hbase.batch.size"
  lazy val HBBASE_COLUMN_FAMILY_KEY = "spark.hbase.column.family"
  lazy val HBASE_MAX_RETRY = "spark.hbase.max.retry"
  lazy val HBASE_CLUSTER_URL = "spark.hbase.cluster"
  lazy val HBASE_DURABILITY = "spark.hbase.durability"
  // fire框架针对hbase操作后数据集的缓存策略，配置列表详见：StorageLevel.scala（配置不区分大小写）
  lazy val SPARK_FIRE_HBASE_STORAGE_LEVEL = "spark.fire.hbase.storage.level"
  // 通过HBase scan后repartition的分区数
  lazy val SPARK_FIRE_HBASE_SCAN_REPARTITIONS = "spark.fire.hbase.scan.repartitions"
  // hbase集群映射配置前缀
  lazy val hbaseClusterMapPrefix = "spark.hbase.cluster.map."

  // hbase集群映射地址
  lazy val hbaseClusterMap: util.Map[String, String] = JavaConversions.mapAsJavaMap(PropUtils.sliceKeys(hbaseClusterMapPrefix))
  // hbase java api 配置前缀
  lazy val hbaseConfPrefix = "spark.hbase.conf."
  // HBase操作默认的批次大小
  lazy val hbaseBatchSize = PropUtils.getInt(this.HBASE_BATCH, 10000)
  // hbase默认的列族名称，如果使用FieldName指定，则会被覆盖
  lazy val familyName = PropUtils.getString(this.HBBASE_COLUMN_FAMILY_KEY, "info")
  // hbase操作失败最大重试次数
  lazy val hbaseMaxRetry = PropUtils.getLong(this.HBASE_MAX_RETRY, 3)
  // hbase集群名称
  lazy val hbaseCluster = PropUtils.getString(this.HBASE_CLUSTER_URL, "")
  lazy val hbaseDurability = PropUtils.getString(this.HBASE_DURABILITY, "")
  // HBase结果集的缓存策略配置
  lazy val hbaseStorageLevel = PropUtils.getString(this.SPARK_FIRE_HBASE_STORAGE_LEVEL, "memory_and_disk_ser").toUpperCase
  // 通过HBase scan后repartition的分区数，默认1200
  lazy val hbaseHadoopScanRepartitions = PropUtils.getInt(this.SPARK_FIRE_HBASE_SCAN_REPARTITIONS, 1200)
}