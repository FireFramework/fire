package com.zto.fire.common.util

import org.apache.commons.lang3.StringUtils

/**
  * 控制台读取工具类
  *
  * @author ChengLong 2019-3-21 10:17:13
  */
object ConsoleUtils {

  /**
    * 从控制台读取一行字符串
    *
    * @param tip
    * 命令行提示语
    * @param defaultStr
    * 默认值
    * @return
    * 读取到的内容
    */
  def readLine(tip: String, defaultStr: String = ""): String = {
    print(tip)
    val line = Console.readLine()
    if (StringUtils.isNotBlank(line)) line.trim else defaultStr
  }

  /**
    * 从控制台读取整型数据
    *
    * @param tip
    * 命令行提示语
    * @param defaultVal
    * 默认值
    * @return
    * 读取到的内容
    */
  def readInt(tip: String, defaultVal: Int = 0): Int = {
    print(tip)
    val value = Console.readInt()
    if (value == null) value else defaultVal
  }

  /**
    * 从控制台读取长整型数据
    *
    * @param tip
    * 命令行提示语
    * @param defaultVal
    * 默认值
    * @return
    * 读取到的内容
    */
  def readLong(tip: String, defaultVal: Long = 0): Long = {
    print(tip)
    val value = Console.readLong()
    if (value == null) value else defaultVal
  }

  /**
    * 从控制台读取布尔值
    *
    * @param tip
    * 命令行提示语
    * @param defaultVal
    * 默认值
    * @return
    * 读取到的内容
    */
  def readBoolean(tip: String, defaultVal: Boolean = false): Boolean = {
    print(tip)
    val value = Console.readBoolean()
    if (value == null) value else defaultVal
  }

}
