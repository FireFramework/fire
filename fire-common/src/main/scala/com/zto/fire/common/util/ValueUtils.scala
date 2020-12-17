package com.zto.fire.common.util

import java.util

import com.zto.fire.predef.JMap
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._

/**
 * 值校验工具，支持任意对象、字符串、集合、map、rdd、dataset是否为空的校验
 *
 * @since 0.4.1
 * @author ChengLong 2019-9-4 13:39:16
 */
private[fire] trait ValueCheck {

  /**
   * 值为空判断，支持任意类型
   *
   * @param param
   * 参数值
   * @return
   * true:empty false:not empty
   */
  def isEmpty(param: Any): Boolean = {
    if (param == null) return true
    param match {
      case str: String => StringUtils.isBlank(str)
      case array: Array[_] => array.isEmpty
      case collection: util.Collection[_] => collection.isEmpty
      case it: Iterable[_] => it.isEmpty
      case map: JMap[_, _] => map.isEmpty
      case _ => false
    }
  }

  /**
   * 值为非空判断，支持任意类型
   *
   * @param param
   * 参数值
   * @return
   * true:not empty false:empty
   */
  def isNotEmpty(param: Any): Boolean = !this.isEmpty(param)

  /**
   * 校验多个参数是否都为空
   *
   * @param params
   * 多个参数
   * @return
   * true：存在为空的参数 false：全都不为空
   */
  def isExistsEmpty(params: Any*): Boolean = {
    if (params == null || params.isEmpty) return true
    params.count(this.isEmpty) > 0
  }

  /**
   * 校验多个参数是否不存在为空的
   *
   * @param params
   * 多个参数
   * @return
   * true：全都不为空 false：存在为空的
   */
  def isAllNotEmpty(params: Any*): Boolean = !this.isExistsEmpty(params: _*)

  /**
   * 参数非空约束（严格模式，进一步验证集合是否有元素）
   *
   * @param params  参数列表信息
   * @param message 异常信息
   */
  def requireNonEmpty(params: Any*)(implicit message: String = "参数不能为空，请检查."): Unit = {
    require(params != null && params.nonEmpty, message)

    var index = 0
    params.foreach(param => {
      index += 1
      param match {
        case str: String => require(StringUtils.isNotBlank(str), s"第[ ${index} ]参数为空，异常信息：$message")
        case array: Array[_] => require(array.nonEmpty, s"第[ ${index} ]参数为空，异常信息：$message")
        case collection: util.Collection[_] => require(!collection.isEmpty, s"第[ ${index} ]参数为空，异常信息：$message")
        case it: Iterable[_] => require(it.nonEmpty, s"第[ ${index} ]参数为空，异常信息：$message")
        case map: JMap[_, _] => require(map.nonEmpty, s"第[ ${index} ]参数为空，异常信息：$message")
      }
    })
  }
}

/**
 * 用于单独调用的值校验工具类
 */
object ValueUtils extends ValueCheck
