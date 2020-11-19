package com.zto.fire.common.util

import com.zto.fire.common.util.ExceptionBus.{stackTrace, tryWithLog}
import org.junit.Test
import org.junit.Assert._

/**
 * 用于ExceptionBus的单元测试
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-16 14:42
 */
class ExceptionBusTest {

  /**
   * 用于测试queue大小限制与exception的存入和获取
   */
  @Test
  def testTry: Unit = {
    (1 to 10020).foreach(i => {
      tryWithLog {
        val a = 1 / 0
      } (isThrow = false)
    })

    val t = ExceptionBus.getAndClear
    assertEquals(t._2.size, 1000)
    t._2.foreach(t => stackTrace(t))

    // 上一次获取后queue中的记录数为0
    assertEquals(ExceptionBus.queueSize.get(), 0)
    assertEquals(ExceptionBus.exceptionCount.get(), 10020)
  }

}
