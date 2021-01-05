package com.zto.fire.common.util

import scala.reflect.{ClassTag, classTag}

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
  def getParamType[T: ClassTag]: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]
}
