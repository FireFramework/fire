package com.zto.fire.common

import org.apache.log4j.{Level, Logger}

/**
 * 单元测试基类
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-11-30 14:39
 */
class UnitTest(loglevel: String = "INFO") {
  Logger.getLogger("com.zto").setLevel(Level.toLevel(this.loglevel))
}
