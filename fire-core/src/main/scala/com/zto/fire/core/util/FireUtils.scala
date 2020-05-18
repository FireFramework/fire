package com.zto.fire.core.util

import java.util.concurrent.atomic.AtomicInteger

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zto.fire.common.bean.ogg.OGGBean
import com.zto.fire.common.util.{DateFormatUtils, GlobalConstants, ValueUtils}
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Try

/**
 * fire框架通用的工具方法
 *
 * @author ChengLong
 * @since 1.0.0
 * @create: 2020-05-17 10:17
 */
object FireUtils {

  /**
   * 重试指定的函数fn retryNum次
   * 当fn执行失败时，会根据设置的重试次数自动重试retryNum次
   * 每次重试间隔等待duration(毫秒)
   *
   * @param retryNum
   * 指定重试的次数
   * @param duration
   * 重试的间隔时间（ms）
   * @param fun
   * 重试的函数或方法
   * @tparam T
   * fn执行后返回的数据类型
   * @return
   * 返回fn执行结果
   */
  def retry[T](retryNum: Long = 3, duration: Long = 3000)(fun: => T): T = {
    var count = 1L

    def redo[T](retryNum: Long, duration: Long)(fun: => T): T = {
      Try {
        fun
      } match {
        case util.Success(x) => x
        case _ if retryNum > 1 => {
          Thread.sleep(duration)
          count += 1
          println(s"${GlobalConstants.PS1.RED}第${count}次执行. 时间:${DateFormatUtils.formatCurrentDateTime()}. 间隔:${duration}.${GlobalConstants.PS1.DEFAULT}")
          redo(retryNum - 1, duration)(fun)
        }
        case util.Failure(e) => throw e
      }
    }

    redo(retryNum, duration)(fun)
  }

  /**
   * 解析ogg中的json数据为指定的JavaBean类型
   *
   * @param json
   * json字符串
   * @param clazz
   * 目标类型
   * @param paseAfter
   * 是否解析after数据
   * @param paseBefore
   * 是否解析before数据
   * @return
   * json解析后的数据
   */
  def oggJsonParse[T: ClassTag](json: String, clazz: Class[T], paseAfter: Boolean = true, paseBefore: Boolean = true): OGGBean[T] = {
    ValueUtils.requireNonNull(json, "ogg消息解析参数不合法：json不能为空")
    ValueUtils.requireNonNull(clazz, "ogg消息解析参数不合法：目标类型不能为空")
    val isJsonArray = StringUtils.trim(json).startsWith("[")
    if (isJsonArray) throw new IllegalArgumentException("ogg消息解析参数不合法：json数据实际为jsonarray")
    val ogg = JSON.parseObject(json, classOf[OGGBean[T]])
    if (ogg != null) {
      if (paseAfter && ogg.getAfter != null) ogg.setAfter(ogg.getAfter.asInstanceOf[JSONObject].toJavaObject(clazz))
      if (paseBefore && ogg.getBefore != null) ogg.setBefore(ogg.getBefore.asInstanceOf[JSONObject].toJavaObject(clazz))
    }

    ogg
  }

  /**
   * 解析ogg中的json数据为指定的JavaBean类型
   *
   * @param json
   * json字符串
   * @param clazz
   * 目标类型
   * @param paseAfter
   * 是否解析after数据
   * @param paseBefore
   * 是否解析before数据
   * @return
   * json解析后的数据
   */
  def oggJsonArrayParse[T: ClassTag](json: String, clazz: Class[T], paseAfter: Boolean = true, paseBefore: Boolean = true): ListBuffer[OGGBean[T]] = {
    ValueUtils.requireNonNull(json, "ogg消息解析参数不合法：json不能为空")
    ValueUtils.requireNonNull(clazz, "ogg消息解析参数不合法：目标类型不能为空")
    val isJsonArray = StringUtils.trim(json).startsWith("[")
    if (!isJsonArray) throw new IllegalArgumentException("ogg消息解析参数不合法：json数据实际为jsonarray")
    val oggList = JSON.parseArray(json, classOf[OGGBean[T]])
    val resultList = ListBuffer[OGGBean[T]]()

    if (oggList != null && oggList.size() > 0) {
      JavaConversions.asScalaBuffer(oggList).foreach(ogg => {
        if (ogg != null) {
          if (paseAfter && ogg.getAfter != null) ogg.setAfter(ogg.getAfter.asInstanceOf[JSONObject].toJavaObject(clazz))
          if (paseBefore && ogg.getBefore != null) ogg.setBefore(ogg.getBefore.asInstanceOf[JSONObject].toJavaObject(clazz))
          resultList += ogg
        }
      })
    }

    resultList
  }
}
