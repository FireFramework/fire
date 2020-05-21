package com.zto.fire.core.rest

import com.zto.fire.core.BaseFire

/**
 * 系统预定义的restful服务抽象
 *
 * @author ChengLong 2020年4月2日 13:58:08
 */
protected[fire] abstract class SystemRestful(engine: BaseFire) {
  protected val module = "restful"
  this.register

  /**
   * 注册接口
   */
  protected def register: Unit
}
