package com.zto.fire.core.ext

import org.slf4j.LoggerFactory

/**
 * 为上层扩展层提供api集合
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-23 17:52
 */
trait Provider {
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)
}
