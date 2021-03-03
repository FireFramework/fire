package com.zto.fire.common.util

import com.zto.fire.common.ext.{JavaExt, ScalaExt}

import scala.collection.convert.{WrapAsJava, WrapAsScala}
import scala.util.control.Breaks

/**
 * 各种工具API的集合类
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-12-16 16:23
 */
trait Tools extends Breaks with TypeMap with ValueCheck with FireFunctions with JavaExt with ScalaExt with ScalaUtils with WrapAsScala with WrapAsJava {

}