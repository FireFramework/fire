package com.zto.fire.common.conf

import com.zto.fire.common.util.GlobalConstants.PropKeys
import com.zto.fire.common.util.PropUtils

/**
 * kudu & impala相关配置
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-13 14:59
 */
class KuduConfiguration extends Enumeration {
  val kuduMaster = PropUtils.getString(PropKeys.KUDU_MASTER_URL)
  val impalaConnectionUrl: String = PropUtils.getString(PropKeys.IMPALA_CONNECTION_URL_KEY)
  val impalaJdbcDriverName: String = PropUtils.getString(PropKeys.IMPALA_JDBC_DRIVER_NAME_KEY)
  val impalaDaemons: String = PropUtils.getString(PropKeys.IMPALA_DAEMONS_URL, "")
}
