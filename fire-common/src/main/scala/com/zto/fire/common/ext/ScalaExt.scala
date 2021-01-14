package com.zto.fire.common.ext

import java.util.regex.Pattern

/**
 * scala相关扩展
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-01-04 10:32
 */
trait ScalaExt {
  // 用于缓存转为驼峰标识的字符串与转换前的字符串的映射关系
  private[this] lazy val humpMap = collection.mutable.Map[String, String]()

  /**
   * String API扩展
   */
  implicit class StringExt[K, V](str: String) {
    // 用于匹配带有下划线字符串的正则
    private[this] lazy val humpPattern = Pattern.compile("(.*)_(\\w)(.*)")

    /**
     * 数据表字段名转换为驼峰式名字的实体类属性名
     *
     * @return 转换后的驼峰式命名
     */
    def toHump: String = {
      val matcher = humpPattern.matcher(str)
      val humpStr = if (matcher.find) {
        (matcher.group(1) + matcher.group(2).toUpperCase + matcher.group(3)).toHump
      } else str
      humpMap += (humpStr -> str)
      humpStr
    }

    /**
     * 驼峰式的实体类属性名转换为数据表字段名
     *
     * @return 转换后的以"_"分隔的数据表字段名
     */
    def unHump: String = humpMap.getOrElse(str, str.replaceAll("[A-Z]", "_$0").toLowerCase)
  }

}

