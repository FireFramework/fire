package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils

/**
 * hive相关配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:02
 */
private[fire] object FireHiveConf {
  lazy val HIVE_CLUSTER = "spark.hive.cluster"
  // hive版本号
  lazy val HIVE_VERSION = "spark.hive.version"
  // hive的catalog名称
  lazy val HIVE_CATALOG_NAME = "spark.hive.catalog.name"
  lazy val HIVE_CLUSTER_MAP_PREFIX = "spark.fire.hive.cluster.map."
  lazy val HIVE_SITE_PATH_MAP_PREFIX = "spark.fire.hive.site.path.map."
  lazy val HIVE_CONF_PREFIX = "spark.hive.conf."

  // hive集群标识（batch/streaming/test）
  lazy val hiveCluster = PropUtils.getString(this.HIVE_CLUSTER, "")
  // 初始化hive集群名称与metastore映射
  private lazy val hiveMetastoreMap = PropUtils.sliceKeys(this.HIVE_CLUSTER_MAP_PREFIX)
  // hive-site.xml存放路径映射
  private lazy val hiveSiteMap = PropUtils.sliceKeys(this.HIVE_SITE_PATH_MAP_PREFIX)
  // hive版本号
  lazy val hiveVersion = PropUtils.getString(this.HIVE_VERSION, "1.1.0")
  // hive catalog名称
  lazy val hiveCatalogName = PropUtils.getString(this.HIVE_CATALOG_NAME, "hive")
  // hive的set配置，如：this.spark.sql("set hive.exec.dynamic.partition=true")
  lazy val hiveConfMap = PropUtils.sliceKeys(this.HIVE_CONF_PREFIX)

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