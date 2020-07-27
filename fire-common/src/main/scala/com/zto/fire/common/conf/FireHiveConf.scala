package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang3.StringUtils

/**
 * hive相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:02
 */
private[fire] object FireHiveConf {
  lazy val HIVE_SUPPORT_ENABLE = "spark.hive.support.enable"
  lazy val HIVE_CLUSTER = "spark.hive.cluster"
  // hive版本号
  lazy val HIVE_VERSION = "spark.hive.version"
  // hive的catalog名称
  lazy val HIVE_CATALOG_NAME = "spark.hive.catalog.name"

  // 是否启用hive支持
  lazy val hiveSupportEnable = PropUtils.getBoolean(this.HIVE_SUPPORT_ENABLE, true)
  // hive集群标识（batch/streaming/test）
  lazy val hiveCluster = PropUtils.getString(this.HIVE_CLUSTER, "")
  // 初始化hive集群名称与metastore映射
  private lazy val hiveMetastoreMap = PropUtils.sliceKeys("spark.hive.cluster.map.")
  // hive-site.xml存放路径映射
  private lazy val hiveSiteMap = PropUtils.sliceKeys("spark.hive.site.path.map.")
  // hive版本号
  lazy val hiveVersion = PropUtils.getString(this.HIVE_VERSION, "1.1.0")
  // hive catalog名称
  lazy val hiveCatalogName = PropUtils.getString(this.HIVE_CATALOG_NAME, "hive")
  // hive的set配置，如：this.spark.sql("set hive.exec.dynamic.partition=true")
  lazy val hiveConfMap = PropUtils.sliceKeys("spark.hive.conf.")

  /**
   * 根据hive集群名称获取metastore地址
   */
  def getMetastoreUrl: String = {
    if (this.hiveMetastoreMap.contains(hiveCluster)) this.hiveMetastoreMap(hiveCluster) else hiveCluster
  }

  /**
   * 获取hive-site.xml的存放路径
   *
   * @return
   * /path/to/hive-site.xml
   */
  def getHiveConfDir: String = {
    if (this.hiveSiteMap.contains(hiveCluster)) this.hiveSiteMap(hiveCluster) else hiveCluster
  }
}