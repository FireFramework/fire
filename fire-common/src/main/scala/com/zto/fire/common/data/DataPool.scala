package com.zto.fire.common.data

import com.zto.fire.common.acc.AccumulatorManager
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
   * 用于构建复杂类型（json）的多时间维度累加器的key
   * 并将key作为多时间维度累加器的key
   *
   * @param value
   * 累加的值
   * @param cluster
   * 连接的集群名
   * @param module
   * 所在的模块
   * @param method
   * 所在的方法名
   * @param action
   * 执行的动作
   * @param sink
   * 作用的目标
   * @param level
   * 日志级别：INFO、ERROR
   * @return
   * 累加器的key（json格式）
   */
  def addMultiTimer(module: String, method: String, action: String, sink: String, level: String, cluster: String, value: Long): Unit = {
    if (GlobalConstants.isSparkEngine) {
      // 目前仅支持spark引擎的埋点数据收集
      AccumulatorManager.addMultiTimer(module, method, action, sink, level, cluster, value)
    }
  }

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
