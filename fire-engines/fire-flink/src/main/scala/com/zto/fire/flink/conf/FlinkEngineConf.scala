package com.zto.fire.flink.conf

import com.zto.fire.core.conf.EngineConf

/**
 * 获取Spark引擎的所有配置信息
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2021-03-02 11:12
 */
private[fire] class FlinkEngineConf extends EngineConf  {

  /**
   * 获取Flink引擎的所有配置信息
   */
  override def getEngineConf: Map[String, String] = {
    Map.empty[String, String]
  }
}
