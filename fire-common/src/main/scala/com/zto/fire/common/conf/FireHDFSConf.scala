package com.zto.fire.common.conf

import com.zto.fire.common.util.PropUtils
import org.apache.commons.lang3.StringUtils

/**
 * HDFS配置
 *
 * @author ChengLong
 * @since 1.1.0
 * @create 2020-07-13 15:07
 */
private[fire] object FireHDFSConf {
  // 是否启用高可用
  lazy val HDFS_HA = "hdfs.ha.enable"
  lazy val HDFS_HA_PREFIX = "hdfs.ha.conf."


  // 配置是否启用hdfs HA
  lazy val hdfsHAEnable = PropUtils.getBoolean(this.HDFS_HA, true)
}