package com.zto.fire.common.util

import java.io.{FileInputStream, InputStream}
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import com.alibaba.fastjson.JSON
import com.zto.fire.common.bean.BaseLogging
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkEnv

import scala.collection.JavaConversions
import scala.collection.mutable.Map

/**
  * 读取配置文件工具类
  * Created by ChengLong on 2016-11-22.
  */
object PropUtils extends BaseLogging {
  private val props = new Properties()
  // 用于判断是否merge过
  private val isMerge = new AtomicBoolean(false)
  // key的前缀
  private var keyPrefix = "spark"
  // 是否兼容key的前缀配置
  private var compatible = false
  // 加载默认配置文件
  this.load("default.properties")

  /**
   * 用于设置兼容的key的前缀
   */
  private[fire] def compatible(keyPrefix: String): Unit = {
    if (StringUtils.isNotBlank(keyPrefix.trim) && !keyPrefix.equals("spark")) {
      this.keyPrefix = keyPrefix.trim
      this.compatible = true
    }
  }

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
          println(s"${GlobalConstants.PS1.YELLOW} --------------------------------- load ${fullName} --------------------------------- ${GlobalConstants.PS1.DEFAULT}")
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
    * 根据key获取配置信息
    *
    * @param key
    * 配置的key
    * @return
    * 配置的value
    */
  def getProperty(key: String): String = {
    if (!this.isMerge.get) this.mergeSparkConf
    if (this.compatible) {
      // 兼容配置key的前缀变化，适配flink.为前缀的配置项
      val value = this.props.getProperty(key.replaceFirst("spark", this.keyPrefix))
      if (StringUtils.isNotBlank(value)) value else this.props.getProperty(key)
    } else {
      this.props.getProperty(key)
    }
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
    * 获取float型数据
    *
    * @param key
    * @return
    */
  def getFloat(key: String): Float = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toFloat else -1
  }

  /**
    * 获取float型数据
    *
    * @param key
    * @return
    */
  def getFloat(key: String, default: Float): Float = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toFloat else default
  }

  /**
    * 获取float型数据
    *
    * @param key
    * @return
    */
  def getDouble(key: String): Double = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toDouble else -1.0
  }

  /**
    * 获取float型数据
    *
    * @param key
    * @return
    */
  def getDouble(key: String, default: Double): Double = {
    val value = this.getProperty(key)
    if (StringUtils.isNotBlank(value)) value.toDouble else default
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
        if (StringUtils.isNotBlank(kv._1) && StringUtils.isNotBlank(kv._2)) {
          this.props.setProperty(kv._1, kv._2)
        }
      })
    }
  }

  /**
    * 设置指定的配置
    *
    * @param key
    * 配置的key
    * @param value
    * 配置的value
    */
  def setProperty(key: String, value: String): Unit = {
    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
      this.props.setProperty(key, value)
    }
  }

  /**
    * 隐蔽密码信息后返回
    *
    * @return
    */
  def cover: Properties = {
    val conf = new Properties()
    JavaConversions.asScalaSet(this.props.keySet()).foreach(key => {
      if (key != null && !key.toString.contains("pass")) {
        conf.setProperty(key.toString, this.props.getProperty(key.toString))
      }
    })
    conf
  }

  /**
    * 打印配置文件中的kv
    */
  def print(): Unit = {
    println(s"${GlobalConstants.PS1.YELLOW} < --------------------------------------- 配置信息 ---------------------------------------- > ${GlobalConstants.PS1.DEFAULT}")
    JavaConversions.asScalaSet(this.props.keySet()).foreach(key => {
      if (key != null && !key.toString.contains("pass")) {
        println(">> " + GlobalConstants.PS1.PINK + key + " --> " + this.props.get(key) + GlobalConstants.PS1.DEFAULT)
      }
    })
    println(s"${GlobalConstants.PS1.YELLOW} < ----------------------------------------------------------------------------------------- > ${GlobalConstants.PS1.DEFAULT}")
  }

  /**
    * 将配置信息转为Map，并设置到SparkConf中
    *
    * @return
    * confMap
    */
  def toMap: Map[String, String] = {
    val confMap = scala.collection.mutable.Map[String, String]()
    JavaConversions.asScalaSet(this.props.keySet()).foreach(key => {
      if (key != null) {
        confMap += (key.toString -> this.props.getProperty(key.toString))
      }
    })
    confMap
  }

  /**
    * 将配置信息转为Map，并设置到Flink Configuration中
    *
    * @return
    * confMap
    */
  def toFlinkConfMap: Map[String, String] = {
    val confMap = scala.collection.mutable.Map[String, String]()
    JavaConversions.asScalaSet(this.props.keySet()).filter(t => t != null && t.toString.startsWith("flink")).foreach(key => {
      if (key != null) {
        confMap += (key.toString -> this.props.getProperty(key.toString))
      }
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
        if (StringUtils.isNotBlank(t._1))
          this.props.setProperty(t._1, t._2)
      })
      this.isMerge.set(true)
    }
  }

  /**
    * 获取zrc配置信息
    */
  def invokeZrcConf(className: String, rest: String): Unit = {
    this.mark
    val param =
      s"""
         |{"className": "$className", "url": "http://$rest", "fireVersion": "${this.getString("spark.fire.version")}", "zrcKey": "21fa30b7f2082b1b12dfbc7c8c6d70b9"}
      """.stripMargin
    this.setProperty("spark.rest.url", s"http://$rest")
    var conf = ""
    try {
      conf = HttpClientUtils.doPost(this.getString("spark.zrc.register.conf.prod.address", "http://192.168.33.199:8080/zrcToExternal/zrcConfCallBack"), param)
    } catch {
      case e: Exception => {
        this.log("调用zrc注册接口失败，开始尝试调用测试环境zrc注册接口。", null, null, e)
        conf = HttpClientUtils.doPost(this.getString("spark.zrc.register.conf.test.address"), param)
      }
    } finally {
      if (StringUtils.isNotBlank(conf)) {
        this.log("成功获取zrc配置信息：" + conf)
        val msg = JSON.parseObject(conf)
        if (msg != null && msg.get("code") == 200) {
          val content = msg.get("content")
          if (content != null) {
            val confMap = JSON.parseObject(content.toString, classOf[java.util.HashMap[String, String]])
            if (confMap != null && conf.size > 0) {
              PropUtils.setProperties(JavaConversions.mapAsScalaMap(confMap))
            }
          }
        }
      }
    }
  }

}
