package com.zto.fire.common.util

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv

import scala.collection.JavaConversions
import scala.collection.mutable.Map

/**
  * 读取配置文件工具类
  * Created by ChengLong on 2016-11-22.
  */
object PropUtils {
  private val props = new Properties()
  // 用于判断是否merge过
  private val isMerge = new AtomicBoolean(false)
  // 加载默认配置文件
  this.load("default.properties")

  /**
    * 加载指定配置文件，resources根目录下优先级最高，其次是按字典顺序的目录
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
        if (resource == null) {
          val findFileName = FindClassUtils.findFileInJar(fullName)
          if (StringUtils.isNotBlank(findFileName)) {
            if (FindClassUtils.isJar) {
              resource = FileUtils.resourceFileExists(findFileName)
            } else {
              resource = new FileInputStream(findFileName)
            }
          }
        }
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

  def loadR(fileName: String): this.type = {
    val url = this.getClass.getResource("/main/resources")
    if (url != null) {
      val file = new File(url.toURI)
    }
    this
  }

  /**
    * 根据key获取配置信息
    *
    * @param key
    * 配置的key
    * @return
    * 配置的value
    */
  def getProperty(key: String): String = {
    if (!this.isMerge.get) this.mergeSparkConf
    this.props.getProperty(key)
  }

  /**
    * 获取字符串
    *
    * @param key
    * @return
    */
  def getString(key: String): String = {
    this.getProperty(key)
  }

  /**
    * 获取拼接后数值的配置字符串
    *
    * @param key    配置的前缀
    * @param keyNum 拼接到key后的数值后缀
    * @return
    * 对应的配置信息
    */
  def getString(key: String, keyNum: Int = 0, default: String = ""): String = {
    if (keyNum == null || keyNum <= 1) {
      var value = this.getProperty(key)
      if (StringUtils.isBlank(value)) {
        value = this.getString(key + "1", default)
      }
      value
    } else {
      this.getString(key + keyNum, default)
    }
  }

  /**
    * 获取字符串，为空则取默认值
    *
    * @param key
    * @return
    */
  def getString(key: String, default: String): String = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value else default
  }

  /**
    * 获取整型数据
    *
    * @param key
    * @return
    */
  def getInt(key: String): Int = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toInt else -1
  }

  /**
    * 获取整型数据
    *
    * @param key
    * @return
    */
  def getInt(key: String, default: Int): Int = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toInt else default
  }

  /**
    * 获取拼接后数值的配置整数
    *
    * @param key    配置的前缀
    * @param keyNum 拼接到key后的数值后缀
    * @return
    * 对应的配置信息
    */
  def getInt(key: String, keyNum: Int = 0, default: Int): Int = {
    val value = this.getString(key, keyNum, default + "")
    if (StringUtils.isNotBlank(value)) value.toInt else default
  }

  /**
    * 获取长整型数据
    *
    * @param key
    * @return
    */
  def getLong(key: String): Long = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toLong else -1L
  }

  /**
    * 获取长整型数据
    *
    * @param key
    * @return
    */
  def getLong(key: String, default: Long): Long = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toLong else default
  }

  /**
    * 获取拼接后数值的配置长整数
    *
    * @param key    配置的前缀
    * @param keyNum 拼接到key后的数值后缀
    * @return
    * 对应的配置信息
    */
  def getLong(key: String, keyNum: Int = 0, default: Long): Long = {
    val value = this.getString(key, keyNum, default + "")
    if (StringUtils.isNotBlank(value)) value.toLong else default
  }

  /**
    * 获取布尔值数据
    *
    * @param key
    * @return
    */
  def getBoolean(key: String): Boolean = {
    val value = this.getProperty(key)
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
    * 获取拼接后数值的配置布尔值
    *
    * @param key    配置的前缀
    * @param keyNum 拼接到key后的数值后缀
    * @return
    * 对应的配置信息
    */
  def getBoolean(key: String, keyNum: Int = 0, default: Boolean): Boolean = {
    val value = this.getString(key, keyNum, default + "")
    if (StringUtils.isNotBlank(value)) value.toBoolean else default
  }

  /**
    * 使用map设置多个值
    *
    * @param map
    * java map，存放多个配置信息
    */
  def setProperties(map: Map[String, String]): Unit = {
    if (map != null) {
      map.foreach(kv => {
        this.props.setProperty(kv._1, kv._2)
      })
    }
  }

  /**
    * 打印配置文件中的kv
    */
  def print(): Unit = {
    println(GlobalConstants.PS1.YELLOW + "< -------------------------------------- 配置信息 -------------------------------------- >" + GlobalConstants.PS1.DEFAULT)
    JavaConversions.asScalaSet(this.props.keySet()).foreach(key => {
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
    JavaConversions.asScalaSet(this.props.keySet()).foreach(key => {
      confMap += (key.toString -> this.props.getProperty(key.toString))
    })
    confMap
  }

  /**
    * 合并SparkConf中的配置信息
    */
  def mergeSparkConf: Unit = {
    val env = SparkEnv.get
    if (env != null && env.conf != null) {
      env.conf.getAll.foreach(t => {
        this.props.setProperty(t._1, t._2)
      })
      this.isMerge.set(true)
    }
  }

  /**
    * 获取zrc配置信息
    */
  def invokeZrcConf(className: String, rest: String): Unit = {
    val param =
      s"""
        |{"className": "$className", "url": "http://$rest", "fireVersion": "${PropUtils.getString("spark.fire.version")}"}
      """.stripMargin

    var conf = ""
    try {
      val url = "http://10.9.38.156:8080/deploy/zrcConfCallBack"
      conf = HttpClientUtils.doPost(url, param)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        val url2 = "http://10.9.38.156:8080/deploy/zrcConfCallBack"
        conf = HttpClientUtils.doPost(url2, param)
      }
    } finally {
      if (StringUtils.isNotBlank(conf)) {
        val map = JSON.parseObject(conf, classOf[java.util.Map[String, String]])
        PropUtils.setProperties(JavaConversions.mapAsScalaMap(map))
      }
    }
  }

}
