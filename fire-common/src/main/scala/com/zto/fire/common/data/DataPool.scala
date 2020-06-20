package com.zto.fire.common.data

import com.zto.fire.common.util.{GlobalConstants, PropUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv

/**
 * 数据池，用于收集埋点数据
 * 隔离引擎间的差异
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-05-26 16:13
 */
object DataPool {

  /**
   * 当传输到executor或taskManager端时进行配置的merge
   */
  private[fire] def mergeConf: Unit = {
    if (GlobalConstants.isSparkEngine) {
      val env = SparkEnv.get
      if (env != null && env.conf != null) {
        env.conf.getAll.foreach(t => {
          if (StringUtils.isNotBlank(t._1))
            PropUtils.setProperty(t._1, t._2)
        })
        PropUtils.isMerge.set(true)
      }
    }
  }
}
