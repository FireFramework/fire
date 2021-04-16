package com.zto.fire.common.util

import scala.reflect.{ClassTag, classTag}
import scala.runtime.Nothing$

/**
 * scala工具类
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-04 14:06
 */
trait ScalaUtils {

  /**
   * 获取泛型具体的类型
   *
   * @tparam T
   * 泛型类型
   * @return
   * Class[T]
   */
  def getParamType[T: ClassTag]: Class[T] = {
    val paramType = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    if (paramType == classOf[Nothing$]) throw new IllegalArgumentException("不合法的方法调用，请在方法调用时指定泛型！")
    paramType
  }
}
