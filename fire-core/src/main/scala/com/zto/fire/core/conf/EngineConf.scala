package com.zto.fire.core.conf

import com.zto.fire.common.util.ReflectionUtils
import org.slf4j.LoggerFactory

import scala.collection.immutable

/**
 * 用于获取不同计算引擎的全局配置信息，同步到fire框架中，并传递到每一个分布式实例
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-03-02 10:48
 */
private[fire] trait EngineConf {

  /**
   * 获取引擎的所有配置信息
   */
  def getEngineConf: Map[String, String]
}

/**
 * 用于获取不同引擎的配置信息
 */
private[fire] object EngineConfHelper {
  private lazy val logger = LoggerFactory.getLogger(this.getClass)
  // 用于记录不同计算引擎配置获取的类信息，当fire支持新的引擎时，需将对应的实现类添加到以下列表中
  private[this] val register = List[String]("com.zto.fire.spark.conf.SparkEngineConf", "com.zto.fire.flink.conf.FlinkEngineConf")

  /**
   * 通过反射获取不同引擎的配置信息
   */
  def getEngineConf: Map[String, String] = {
    var clazz: Class[_] = null
    for (i <- register.indices) {
      try {
        clazz = Class.forName(register(i))
      } catch {
        case e => logger.info(s"未找到引擎配置获取实现类${register(i)}，将继续重试")
      }
    }

    if (clazz != null) {
      val method = clazz.getDeclaredMethod("getEngineConf")
      ReflectionUtils.setAccessible(method)
      method.invoke(clazz.newInstance()).asInstanceOf[immutable.Map[String, String]]
    } else Map.empty
  }

}
