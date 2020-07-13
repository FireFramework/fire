package com.zto.fire.common.conf

import com.zto.fire.common.util.GlobalConstants.PropKeys
import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang3.StringUtils

/**
 * hive相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:02
 */
class HiveConfiguration extends Enumeration {
  // 是否启用hive支持
  lazy val hiveSupportEnable = PropUtils.getBoolean(PropKeys.HIVE_SUPPORT_ENABLE, true)
  // hive集群标识（batch/streaming/test）
  lazy val hiveCluster = PropUtils.getString(PropKeys.HIVE_CLUSTER, "")
  // 初始化hive集群名称与metastore映射
  private lazy val hiveMetastoreMap = PropUtils.sliceKeys("spark.hive.cluster.map.")
  // hive-site.xml存放路径映射
  private lazy val hiveSiteMap = PropUtils.sliceKeys("spark.hive.site.path.map.")
  // hive版本号
  lazy val hiveVersion = PropUtils.getString(PropKeys.HIVE_VERSION, "1.1.0")
  // hive catalog名称
  lazy val hiveCatalogName = PropUtils.getString(PropKeys.HIVE_CATALOG_NAME, "hive")
  // hive的set配置，如：this.spark.sql("set hive.exec.dynamic.partition=true")
  lazy val hiveConfMap = PropUtils.sliceKeys("spark.hive.conf.")

  /**
   * 根据hive集群名称获取metastore地址
   */
  def getMetastoreUrl: String = {
    val metastore = if (StringUtils.isNotBlank(hiveCluster) && hiveCluster.contains(":")) {
      hiveCluster
    } else if (this.hiveMetastoreMap.contains(hiveCluster)) {
      this.hiveMetastoreMap.get(hiveCluster).get
    } else {
      ""
    }
    metastore
  }

  /**
   * 获取hive-site.xml的存放路径
   *
   * @return
   * /path/to/hive-site.xml
   */
  def getHiveConfDir: String = {
    val hiveSitePath = if (StringUtils.isNotBlank(hiveCluster) && hiveCluster.contains("""/""")) {
      hiveCluster
    } else if (this.hiveSiteMap.contains(hiveCluster)) {
      this.hiveSiteMap.get(hiveCluster).get
    } else {
      throw new IllegalArgumentException(s"未找到匹配的hive-site.xml存放路径，请检查参数：flink.hive.cluster")
    }
    hiveSitePath
  }
}