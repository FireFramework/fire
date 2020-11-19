package com.zto.fire.common.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zto.fire.common.bean.HBaseBaseBean
import com.zto.fire.common.bean.ogg.OGGBean
import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import com.zto.fire.common.util.UnitFormatUtils._

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Try

/**
 * fire框架通用的工具方法
 * 注：该工具类中不可包含Spark或Flink的依赖
 *
 * @author ChengLong
 * @since 1.0.0
 * @create: 2020-05-17 10:17
 */
private[fire] object FireUtils extends Serializable {
  private var isSplash = false
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

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
          println(s"${FirePS1Conf.RED}第${count}次执行. 时间:${DateFormatUtils.formatCurrentDateTime()}. 间隔:${duration}.${FirePS1Conf.DEFAULT}")
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

    this.buildOggBean(clazz, paseAfter, paseBefore, ogg)
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
    ValueUtils.requireNonNull(json, "ogg消息解析参数不合法：json array不能为空")
    ValueUtils.requireNonNull(clazz, "ogg消息解析参数不合法：目标类型不能为空")

    val isJsonArray = StringUtils.trim(json).startsWith("[")
    if (!isJsonArray) throw new IllegalArgumentException("ogg消息解析参数不合法：json数据实际为json array")
    val oggList = JSON.parseArray(json, classOf[OGGBean[T]])
    val resultList = ListBuffer[OGGBean[T]]()

    if (oggList != null && oggList.size() > 0) {
      JavaConversions.asScalaBuffer(oggList).foreach(ogg => {
        if (ogg != null) {
          resultList += this.buildOggBean(clazz, paseAfter, paseBefore, ogg)
        }
      })
    }

    resultList
  }

  /**
   * 工具方法构建ogg的after与before字段
   */
  private def buildOggBean[T: ClassTag](clazz: Class[T], paseAfter: Boolean, paseBefore: Boolean, ogg: OGGBean[T]): OGGBean[T] = {
    // 如果是HBaseBaseBean子类，则调用buildRowKey方法
    def buildRowKey(afterObj: T): Unit = {
      if (afterObj.isInstanceOf[HBaseBaseBean[T]]) {
        val method = clazz.getDeclaredMethod("buildRowKey")
        if (method != null) {
          method.setAccessible(true)
          method.invoke(afterObj)
        }
      }
    }

    if (ogg != null) {
      if (paseAfter && ogg.getAfter != null) {
        val afterObj = ogg.getAfter.asInstanceOf[JSONObject].toJavaObject(clazz)
        if (afterObj != null) {
          buildRowKey(afterObj)
          ogg.setAfter(afterObj)
        }
      }

      if (paseBefore && ogg.getBefore != null) {
        val beforeObj = ogg.getBefore.asInstanceOf[JSONObject].toJavaObject(clazz)
        if (beforeObj != null) {
          buildRowKey(beforeObj)
          ogg.setBefore(beforeObj)
        }
      }
    }

    ogg
  }

  /**
   * 判断是否为spark引擎
   */
  def isSparkEngine: Boolean = "spark".equals(PropUtils.engine)

  /**
   * 判断是否为flink引擎
   */
  def isFlinkEngine: Boolean = "flink".equals(PropUtils.engine)

  /**
   * 用于在fire框架启动时展示信息
   */
  private[fire] def splash: Unit = {
    if (!isSplash) {
      val info =
        """
          |       ___                       ___           ___
          |     /\  \          ___        /\  \         /\  \
          |    /::\  \        /\  \      /::\  \       /::\  \
          |   /:/\:\  \       \:\  \    /:/\:\  \     /:/\:\  \
          |  /::\~\:\  \      /::\__\  /::\~\:\  \   /::\~\:\  \
          | /:/\:\ \:\__\  __/:/\/__/ /:/\:\ \:\__\ /:/\:\ \:\__\
          | \/__\:\ \/__/ /\/:/  /    \/_|::\/:/  / \:\~\:\ \/__/
          |      \:\__\   \::/__/        |:|::/  /   \:\ \:\__\
          |       \/__/    \:\__\        |:|\/__/     \:\ \/__/
          |                 \/__/        |:|  |        \:\__\
          |                               \|__|         \/__/     version
          |
          |""".stripMargin.replace("version", s"version ${FirePS1Conf.PINK + FireFrameworkConf.fireVersion}")

      this.logger.warn(FirePS1Conf.GREEN + info + FirePS1Conf.DEFAULT)
      this.isSplash = true
    }
  }

  /**
   * 获取当前系统时间（ms）
   */
  def currentTime: Long = System.currentTimeMillis

  /**
   * 以人类可读的方式计算耗时（ms）
   * @param beginTime
   * @return
   */
  def timecost(beginTime: Long): String = readable(currentTime - beginTime, TimeUnitEnum.ms)

}
