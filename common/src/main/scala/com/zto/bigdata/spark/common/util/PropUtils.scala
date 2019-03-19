package com.zto.bigdata.spark.common.util

import java.io.InputStream
import java.util.Properties

import org.apache.commons.lang3.StringUtils

/**
  * 读取配置文件工具类
  * Created by ChengLong on 2016-11-22.
  */
object PropUtils {
  private val props = new Properties()
  props.load(this.getClass.getClassLoader.getResourceAsStream("conf.properties"))

  /**
    * 加载指定配置文件
    *
    * @param inStream
    */
  def load(inStream: InputStream): this.type = {
    props.load(inStream)
    this
  }

  /**
    * 获取字符串
    *
    * @param key
    * @return
    */
  def getString(key: String): String = {
    props.getProperty(key)
  }

  /**
    * 获取字符串，为空则取默认值
    *
    * @param key
    * @return
    */
  def getString(key: String, default: String): String = {
    val value = props.getProperty(key)
    if (StringUtils.isNotBlank(value)) value else default
  }

  /**
    * 获取整型数据
    *
    * @param key
    * @return
    */
  def getInt(key: String): Int = {
    val value = props.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toInt else -1
  }

  /**
    * 获取长整型数据
    *
    * @param key
    * @return
    */
  def getLong(key: String): Long = {
    val value = props.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toLong else -1L
  }

  /**
    * 获取布尔值数据
    *
    * @param key
    * @return
    */
  def getBoolean(key: String): Boolean = {
    val value = props.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toBoolean else false
  }
}
