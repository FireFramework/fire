package com.zto.fire.common.util

import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.event.Level

/**
 * 日志工具类
 *
 * @author ChengLong
 * @since 1.0.0
 * @create 2020-07-01 10:23
 */
object LogUtils {

  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 以固定的开始与结束风格打日志
   *
   * @param logger
   * 日志记录器
   * @param title
   * 日志开始标题
   * @param style
   * 日志开始标题类型
   * @param level
   * 日志的级别
   * @param fun
   * 用户自定义的操作
   */
  def logStyle(logger: Logger, title: String = "", style: String = "-", level: Level = Level.WARN)(fun: Logger => Unit): Unit = {
    val styleRepeat = StringUtils.repeat(style, 19)
    val titleStart = styleRepeat + s"${GlobalConstants.PS1.GREEN}> start: " + title + s" <${GlobalConstants.PS1.DEFAULT}" + styleRepeat
    this.logLevel(logger, titleStart, level)
    fun(logger)
    val titleEnd = styleRepeat + s"${GlobalConstants.PS1.GREEN}> end:   " + title + s" <${GlobalConstants.PS1.DEFAULT}" + styleRepeat
    this.logLevel(logger, titleEnd, level)
  }

  /**
   * 以固定的风格打印map中的内容
   */
  def logMap(logger: Logger, map: Map[_, _], title: String): Unit = {
    if (logger != null && map != null && map.nonEmpty) {
      LogUtils.logStyle(this.logger, title)(logger => {
        map.foreach(kv => logger.warn(s"---> ${kv._1} = ${kv._2}"))
      })
    }
  }

  /**
   * 根据指定的基本进行日志记录
   *
   * @param logger
   * 日志记录器
   * @param log
   * 日志内容
   * @param level
   * 日志的级别
   */
  def logLevel(logger: Logger, log: String, level: Level = Level.INFO, ps: String = null): Unit = {
    val logMsg = if (StringUtils.isNotBlank(ps)) s"$ps $log ${GlobalConstants.PS1.DEFAULT}" else log
    level match {
      case Level.DEBUG => logger.debug(logMsg)
      case Level.INFO => logger.info(logMsg)
      case Level.WARN => logger.warn(logMsg)
      case Level.ERROR => logger.error(logMsg)
      case Level.TRACE => logger.trace(logMsg)
    }
  }
}
