package com.zto.fire.core

import com.zto.fire.common.util.PropUtils

/**
 * Flink引擎通用父接口
 *
 * @author ChengLong 2020年1月7日 09:31:09
 */
trait BaseFlink extends BaseFire {

  /**
   * 生命周期方法：初始化fire框架必要的信息
   * 注：该方法会同时在driver端与executor端执行
   */
  override private[fire] def boot: Unit = {
    PropUtils.compatible("flink")
    PropUtils.load("flink")
    PropUtils.load(this.appName)
    this.splash
  }

  /**
   * 生命周期方法：用于在SparkSession初始化之前完成用户需要的动作
   * 注：该方法会在进行init之前自动被系统调用
   *
   * @param args
   * main方法参数
   */
  override def before(args: Array[String]): Unit = {}

  /**
   * 生命周期方法：初始化运行信息
   *
   * @param conf 配置信息
   * @param args main方法参数
   */
  override def init(conf: Any, args: Array[String]): Unit = {}

  /**
   * 生命周期方法：用于资源回收与清理，子类复写实现具体逻辑
   * 注：该方法会在进行destroy之前自动被系统调用
   */
  override def after(args: Array[String]): Unit = {}


  /**
   * 生命周期方法：具体的用户开发的业务逻辑代码
   * 注：此方法会被自动调用，不需要在main中手动调用
   */
  override def process: Unit = {}

  /**
   * 生命周期方法：用于回收资源
   */
  override def stop: Unit = {}

  /**
   * 生命周期方法：进行fire框架的资源回收
   * 注：不允许子类覆盖
   */
  override private[fire] def shutdown(stopGracefully: Boolean): Unit = {}
}
