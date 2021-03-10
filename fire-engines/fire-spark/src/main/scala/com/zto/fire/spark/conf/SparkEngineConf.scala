package com.zto.fire.spark.conf

import com.zto.fire.core.conf.EngineConf
import com.zto.fire.spark.util.SparkUtils
import org.apache.spark.SparkEnv

/**
 * 获取Spark引擎的所有配置信息
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-03-02 10:57
 */
private[fire] class SparkEngineConf extends EngineConf {

  /**
   * 获取引擎的所有配置信息
   */
  override def getEngineConf: Map[String, String] = {
    if (SparkUtils.isExecutor) {
      SparkEnv.get.conf.getAll.toMap
    } else {
      Map.empty[String, String]
    }
  }
}