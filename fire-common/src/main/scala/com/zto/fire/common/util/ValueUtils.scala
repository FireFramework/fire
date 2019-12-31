package com.zto.fire.common.util

import java.util
import java.util.Objects

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * 值校验工具，支持任意对象、字符串、集合、map、rdd、dataset是否为空的校验
 *
 * @since 0.4.1
 * @author ChengLong 2019-9-4 13:39:16
 */
object ValueUtils {

  /**
   * 用于校验java的集合是否为空
   *
   * @param collection
   * java集合
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(collection: util.Collection[_]): Boolean = collection == null || collection.isEmpty

  /**
   * 用于校验java的集合是否不为空
   *
   * @param collection
   * java集合
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(collection: util.Collection[_]): Boolean = !this.isEmpty(collection)

  /**
   * 用于校验java的map集合是否为空
   *
   * @param map
   * java的map集合
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(map: java.util.Map[_, _]): Boolean = map == null || map.isEmpty

  /**
   * 用于校验java的map集合是否不为空
   *
   * @param map
   * java的map集合
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(map: java.util.Map[_, _]): Boolean = !this.isEmpty(map)

  /**
   * 用于校验数组是否为空
   *
   * @param array
   * 数组
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(array: Array[Any]): Boolean = array == null || array.length == 0

  /**
   * 用于校验数组是否不为空
   *
   * @param array
   * 数组
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(array: Array[Any]): Boolean = !this.isEmpty(array)

  /**
   * 用于校验scala的集合是否为空
   *
   * @param seq
   * scala集合
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(seq: Seq[_]): Boolean = seq == null || seq.isEmpty

  /**
   * 用于校验scala的集合是否不为空
   *
   * @param seq
   * scala集合
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(seq: Seq[_]): Boolean = !this.isEmpty(seq)

  /**
   * 用于校验scala的map集合是否为空
   *
   * @param map
   * map集合
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(map: scala.collection.Map[_, _]) = map == null || map.isEmpty

  /**
   * 用于校验scala的map集合是否不为空
   *
   * @param map
   * map集合
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(map: scala.collection.Map[_, _]): Boolean = !this.isEmpty(map)

  /**
   * 用于校验字符串是否为空
   *
   * @param str
   * 字符串
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(str: String): Boolean = StringUtils.isBlank(str)

  /**
   * 用于校验字符串是否不为空
   *
   * @param str
   * 字符串
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(str: String): Boolean = !this.isEmpty(str)

  /**
   * 用于校验java的集合是否为空
   *
   * @param any
   * scala对象
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(any: Any) = any == null

  /**
   * 用于校验java的集合是否不为空
   *
   * @param any
   * scala对象
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(any: Any): Boolean = !isEmpty(any)

  /**
   * 用于校验rdd是否为空
   *
   * @param rdd
   * scala对象
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(rdd: RDD[_]): Boolean = rdd == null || rdd.isEmpty()

  /**
   * 用于校验rdd是否不为空
   *
   * @param rdd
   * scala对象
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(rdd: RDD[_]): Boolean = !isEmpty(rdd)

  /**
   * 用于校验Dataset是否为空
   *
   * @param ds
   * scala对象
   * @return
   * true: 为空  false：不为空
   */
  def isEmpty(ds: Dataset[_]): Boolean = ds == null || ds.rdd.isEmpty()

  /**
   * 用于校验Dataset是否不为空
   *
   * @param ds
   * scala对象
   * @return
   * true: 不为空  false：为空
   */
  def isNotEmpty(ds: Dataset[_]): Boolean = !isEmpty(ds)

  /**
   * 校验多个参数是否都为空
   *
   * @param params
   * 多个参数
   * @return
   * true：存在为空的参数 false：全都不为空
   */
  def isExistsEmpty(params: Any*): Boolean = {
    if (params == null || params.length == 0) return true
    for (param <- params) {
      if (param == null) return true
    }
    false
  }

  /**
   * 校验多个参数都不为空
   *
   * @param params
   * 多个参数
   * @return
   * true：全都不为空 false：存在为空的
   */
  def isExistsNotEmpty(params: Any*): Boolean = !isExistsEmpty(params)

  /**
   * 校验多个参数是否存在为空
   *
   * @param params
   * 多个参数
   * @return
   * true：存在为空的参数 false：全都不为空
   */
  def isExistsEmpty(params: Array[Any]): Boolean = {
    if (params == null || params.isEmpty) return true

    params.foreach(param => {
      if (param == null) return true
    })

    false
  }

  /**
   * 校验多个参数都不为空
   *
   * @param params
   * 多个参数
   * @return
   * true：全都不为空 false：存在为空的
   */
  def isExistsNotEmpty(params: Array[Any]): Boolean = !isExistsEmpty(params)

  /**
   * 参数非空约束
   *
   * @param param   参数信息
   * @param message 异常信息
   */
  def requireNonNull(param: Any, message: String): Unit = Objects.requireNonNull(param, message)

  /**
   * 参数必须为空约束
   *
   * @param param   参数信息
   * @param message 异常信息
   */
  def requireNull(param: Any, message: String): Unit = if (param != null) throw new IllegalArgumentException(message)

  /**
   * 参数非空约束（严格模式，进一步验证集合是否有元素）
   *
   * @param param   参数信息
   * @param message 异常信息
   */
  def requireNonNullForce(param: Any, message: String): Unit = {
    requireNonNull(param, message)
    if (param.isInstanceOf[String] && this.isEmpty(param.asInstanceOf[String])) throw new IllegalArgumentException(message)
    else if (param.isInstanceOf[util.Collection[_]] && param.asInstanceOf[util.Collection[_]].size == 0) throw new IllegalArgumentException(message)
    else if (param.isInstanceOf[Map[_, _]] && param.asInstanceOf[Map[_, _]].size == 0) throw new IllegalArgumentException(message)
  }

  /**
   * 用于严格校验当前DataFrame是否来自Streaming Source
   *
   * @param dataFrame
   */
  def requireStreaming(dataFrame: DataFrame, api: String = ""): Unit = {
    if (!dataFrame.isStreaming) throw new RuntimeException(s"不合法的API调用，必须在structured streaming中调用此方法 $api")
  }

  /**
   * 用于严格校验当前DataFrame是否来自非Streaming Source
   *
   * @param dataFrame
   */
  def requireNotStreaming(dataFrame: DataFrame, api: String = ""): Unit = {
    if (dataFrame.isStreaming) throw new RuntimeException(s"不合法的API调用，不允许在structured streaming中调用此方法 $api")
  }
}
