package com.zto.fire.flink.core

import com.zto.fire.common.task.SchedulerManager
import com.zto.fire.common.util.{GlobalConstants, PropUtils}
import com.zto.fire.core.BaseFire
import com.zto.fire.core.rest.{RestfulRegister, SparkSystemRestful}
import com.zto.fire.flink.core.rest.FlinkSystemRestful
import com.zto.fire.flink.core.util.FlinkSingletonFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * Flink引擎通用父接口
 *
 * @author ChengLong 2020年1月7日 09:31:09
 */
trait BaseFlink extends BaseFire {
  protected var conf: Configuration = _
  protected var hive: HiveCatalog = _

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  override private[fire] def boot: Unit = {
    PropUtils.compatible("flink")
    PropUtils.load("flink")
    PropUtils.load(this.appName)
    FlinkSingletonFactory.setAppName(this.appName)
    this.splash
  }

  /**
   * 初始化flink运行时环境
   */
  override private[fire] def createContext(conf: Any): Unit = {
    this.restfulRegister = new RestfulRegister(this.threadPool).port(restPort)
    this.systemRestful = new FlinkSystemRestful(this)

    SchedulerManager.registerTasks(this)
    // 创建HiveCatalog
    this.hive = new HiveCatalog("hive", GlobalConstants.SparkConf.defaultDB, GlobalConstants.HiveConf.hiveSiteDir, GlobalConstants.HiveConf.hiveVersion)
  }

  /**
   * 构建或合并Configuration
   * 注：不同的子类需根据需要复写该方法
   *
   * @param conf
   * 在conf基础上构建
   * @return
   * 合并后的Configuration对象
   */
  def buildConf(conf: Configuration): Configuration

  /**
   * 生命周期方法：用于回收资源
   */
  override def stop: Unit = {
    try {
      this.after()
      // TODO: stop flink相关的上下文

    } finally {
      this.shutdown()
    }
  }

  /**
   * 生命周期方法：进行fire框架的资源回收
   * 注：不允许子类覆盖
   */
  override private[fire] def shutdown(stopGracefully: Boolean = true): Unit = {
    super.shutdown(stopGracefully)
  }
}
