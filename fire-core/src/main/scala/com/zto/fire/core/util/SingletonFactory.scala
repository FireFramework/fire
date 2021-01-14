package com.zto.fire.core.util

import com.zto.fire.common.util.ValueUtils

/**
 * 单例工厂
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-18 14:02
 */
private[fire] trait SingletonFactory {
  @transient protected[this] var appName: String = _

  /**
   * 设置TableEnv实例
   */
  protected[fire] def setAppName(appName: String): this.type = {
    if (ValueUtils.noEmpty(appName) && ValueUtils.isEmpty(this.appName)) this.appName = appName
    this
  }
}
