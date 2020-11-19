package com.zto.fire.common.conf

import java.util

import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang.StringUtils

import scala.collection.JavaConversions

/**
 * hbase相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:08
 */
private[fire] object FireHBaseConf {
  lazy val HBASE_BATCH = "spark.fire.hbase.batch.size"
  lazy val HBBASE_COLUMN_FAMILY_KEY = "spark.hbase.column.family"
  lazy val HBASE_MAX_RETRY = "spark.hbase.max.retry"
  lazy val HBASE_CLUSTER_URL = "spark.hbase.cluster"
  lazy val HBASE_DURABILITY = "spark.hbase.durability"
  // fire框架针对hbase操作后数据集的缓存策略，配置列表详见：StorageLevel.scala（配置不区分大小写）
  lazy val SPARK_FIRE_HBASE_STORAGE_LEVEL = "spark.fire.hbase.storage.level"
  // 通过HBase scan后repartition的分区数
  @deprecated("use spark.fire.hbase.scan.partitions", "v1.0.0")
  lazy val SPARK_FIRE_HBASE_SCAN_REPARTITIONS = "spark.fire.hbase.scan.repartitions"
  lazy val SPARK_FIRE_HBASE_SCAN_PARTITIONS = "spark.fire.hbase.scan.partitions"
  // hbase集群映射配置前缀
  lazy val hbaseClusterMapPrefix = "spark.fire.hbase.cluster.map."

  // hbase集群映射地址
  lazy val hbaseClusterMap: util.Map[String, String] = JavaConversions.mapAsJavaMap(PropUtils.sliceKeys(hbaseClusterMapPrefix))
  // hbase java api 配置前缀
  lazy val hbaseConfPrefix = "spark.fire.hbase.conf."

  // HBase操作默认的批次大小
  def hbaseBatchSize(keyNum: Int = 1): Int = PropUtils.getInt(this.HBASE_BATCH, keyNum, 10000)

  // hbase默认的列族名称，如果使用FieldName指定，则会被覆盖
  def familyName(keyNum: Int = 1): String = PropUtils.getString(this.HBBASE_COLUMN_FAMILY_KEY, keyNum, "info")

  // hbase操作失败最大重试次数
  def hbaseMaxRetry(keyNum: Int = 1): Long = PropUtils.getLong(this.HBASE_MAX_RETRY, keyNum, 3)

  // hbase集群名称
  def hbaseCluster(keyNum: Int = 1): String = PropUtils.getString(this.HBASE_CLUSTER_URL, keyNum, "")

  /**
   * 根据给定的HBase集群别名获取对应的hbase.zookeeper.quorum地址
   */
  def hbaseClusterUrl(keyNum: Int = 1): String = {
    val clusterName = FireHBaseConf.hbaseCluster()
    if (FireHBaseConf.hbaseClusterMap.containsKey(clusterName)) FireHBaseConf.hbaseClusterMap.get(clusterName) else clusterName
  }

  def hbaseDurability(keyNum: Int = 1): String = PropUtils.getString(this.HBASE_DURABILITY, keyNum, "")

  // HBase结果集的缓存策略配置
  def hbaseStorageLevel: String = PropUtils.getString(this.SPARK_FIRE_HBASE_STORAGE_LEVEL, "memory_and_disk_ser").toUpperCase

  // 通过HBase scan后repartition的分区数，默认1200
  def hbaseHadoopScanPartitions: Int = {
    val partitions = PropUtils.getInt(this.SPARK_FIRE_HBASE_SCAN_PARTITIONS, -1)
    if (partitions != -1) partitions else PropUtils.getInt(this.SPARK_FIRE_HBASE_SCAN_REPARTITIONS, 1200)
  }
}