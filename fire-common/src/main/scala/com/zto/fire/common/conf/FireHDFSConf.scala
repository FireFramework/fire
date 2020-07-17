package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration

/**
 * HDFS配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:07
 */
private[fire] object FireHDFSConf {
  // 是否启用高可用
  lazy val HDFS_HA = "spark.hdfs.ha.enable"
  lazy val HDFS_HA_PREFIX = "spark.hdfs.ha.conf."


  // 配置是否启用hdfs HA
  lazy val hdfsHAEnable = PropUtils.getBoolean(this.HDFS_HA, true)

  /**
   * hdfs高可用关联hive集群
   */
  def linkHiveCluster(hadoopConf: Configuration): Unit = {
    if (hadoopConf != null && this.hdfsHAEnable) {
      val hdfsHAConf = PropUtils.sliceKeys(s"${this.HDFS_HA_PREFIX}${FireHiveConf.hiveCluster}.")
      hdfsHAConf.foreach(kv => {
        if (StringUtils.isBlank(kv._2)) throw new IllegalArgumentException(s"hdfs HA参数不合法，请检查配置项：${kv._1}")
        hadoopConf.set(kv._1, kv._2)
      })
    }
  }
}