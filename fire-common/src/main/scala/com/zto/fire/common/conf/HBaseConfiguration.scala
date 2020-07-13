package com.zto.fire.common.conf

import com.zto.fire.common.util.GlobalConstants.{DefaultVals, PropKeys}
import com.zto.fire.common.util.PropUtils

/**
 * hbase相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 15:08
 */
class HBaseConfiguration extends Enumeration {
  // HBase操作默认的批次大小
  lazy val hbaseBatchSize = PropUtils.getInt(PropKeys.HBASE_BATCH, DefaultVals.hbaseBatch)
}
