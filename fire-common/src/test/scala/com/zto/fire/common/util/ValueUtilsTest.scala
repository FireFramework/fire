package com.zto.fire.common.util

import com.zto.fire.predef._
import org.junit.Test

/**
 * ValueUtils工具类单元测试
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-16 13:21
 */
class ValueUtilsTest {

  /**
   * 测试isEmpty、isNotEmpty等API
   */
  @Test
  def testIsEmpty(): Unit = {
    val str = ""
    assert(isEmpty(str), "字符串不能为空")
    val map = new JHashMap[String, Integer]()
    assert(isEmpty(str, map), "存在为空的值")
    map.put("1", 1)
    assert(noEmpty("123", map), "都不为空")
    assert(!noEmpty("123", map, ""), "存在为空的")
  }

  /**
   * 测试参数检测API
   */
  @Test
  def testRequireNonEmpty(): Unit = {
    val arr = new Array[Int](1)
    val map = Map("str" -> 1)
    val mutableMap = scala.collection.mutable.Map("str" -> 1)
    val jmap = new JHashMap[String, Integer]()
    jmap.put("str", 1)
    val jset = new JHashSet[Int]()
    jset.add(1)
    requireNonEmpty(arr, map, mutableMap, jmap, jset)("参数不合法")
  }
}
