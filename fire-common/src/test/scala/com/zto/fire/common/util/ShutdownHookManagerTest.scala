package com.zto.fire.common.util

import org.apache.log4j.{Level, Logger}
import org.junit.Test

/**
 * shutdown hook管理器单元测试
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-20 14:45
 */
class ShutdownHookManagerTest {
  Logger.getLogger(classOf[ShutdownHookManagerTest]).setLevel(Level.toLevel("INFO"))

  @Test
  def testRegister: Unit = {
    ShutdownHookManager.addShutdownHook(1) {
      () => println("1. 执行逻辑")
    }
    ShutdownHookManager.addShutdownHook(3) {
      () => println("3. 执行逻辑")
    }
    ShutdownHookManager.addShutdownHook(2) {
      () => println("2. 执行逻辑")
    }
    ShutdownHookManager.addShutdownHook(5) {
      () => println("5. 执行逻辑")
    }
    println("=========main method==========")
  }
}
