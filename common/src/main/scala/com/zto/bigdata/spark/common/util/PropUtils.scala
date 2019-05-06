package com.zto.bigdata.spark.common.util

import java.io.InputStream
import java.util.Properties

import org.apache.commons.lang3.StringUtils
import com.zto.bigdata.spark.common.ext.ScalaExt._
import scala.collection.mutable.Map

/**
  * 读取配置文件工具类
  * Created by ChengLong on 2016-11-22.
  */
object PropUtils {
  private val props = new Properties()
  // 加载默认配置文件
  this.load("default.properties")

  /**
    * 加载指定配置文件
    *
    * @param fileName
    * 配置文件名称
    */
  def load(fileName: String): this.type = {
    if (StringUtils.isNotBlank(fileName)) {
      val fullName = if (fileName.endsWith(".properties")) fileName else s"$fileName.properties"
      var resource: InputStream = null
      try {
        resource = FileUtils.resourceFileExists(fullName)
        if (resource != null) {
          println(s"--------------------- load ${fullName} ---------------------")
          props.load(resource)
        }
      } finally {
        if (resource != null) {
          IOUtils.close(resource)
        }
      }
    }
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

  /**
    * 获取布尔值数据
    *
    * @param key
    * @return
    */
  def getBoolean(key: String, default: Boolean): Boolean = {
    val value = this.getBoolean(key)
    if (value != null) value else default
  }

  /**
    * 打印配置文件中的kv
    */
  def print(): Unit = {
    println(GlobalConstants.PS1.YELLOW + "< -------------------------------------- 配置信息 -------------------------------------- >" + GlobalConstants.PS1.DEFAULT)
    this.props.keySet().toScalaSet.foreach(key => {
      println(">> " + GlobalConstants.PS1.PINK + key + " --> " + this.props.get(key) + GlobalConstants.PS1.DEFAULT)
    })
    println(GlobalConstants.PS1.YELLOW + "< -------------------------------------------------------------------------------------- >" + GlobalConstants.PS1.DEFAULT)
  }

  /**
    * 将配置信息转为Map
    *
    * @return
    * confMap
    */
  def toMap: Map[String, String] = {
    val confMap = scala.collection.mutable.Map[String, String]()
    this.props.keySet().toScalaSet.foreach(key => {
      confMap += (key.toString -> this.props.getProperty(key.toString))
    })
    confMap
  }
}
