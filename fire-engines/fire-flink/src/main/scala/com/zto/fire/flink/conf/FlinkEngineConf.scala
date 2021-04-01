package com.zto.fire.flink.conf

import com.zto.fire.common.util.ReflectionUtils
import com.zto.fire.core.conf.EngineConf
import com.zto.fire.flink.util.FlinkUtils
import com.zto.fire.predef._

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
    if (FlinkUtils.isJobManager) {
      // 如果是JobManager端，则需将flink参数和用户参数进行合并，并从合并后的settings中获取
      val clazz = Class.forName("org.apache.flink.configuration.GlobalConfiguration")
      if (ReflectionUtils.containsMethod(clazz, "getSettings")) {
        return clazz.getMethod("getSettings").invoke(null).asInstanceOf[JMap[String, String]].toMap
      }
    } else if (FlinkUtils.isTaskManager) {
      // 如果是TaskManager端，则flink会通过EnvironmentInformation将参数进行传递
      val clazz = Class.forName("org.apache.flink.runtime.util.EnvironmentInformation")
      if (ReflectionUtils.containsMethod(clazz, "getSettings")) {
        return clazz.getMethod("getSettings").invoke(null).asInstanceOf[JMap[String, String]].toMap
      }
    }
    new JHashMap[String, String]().toMap
  }
}
