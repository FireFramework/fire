package com.zto.bigdata.spark.common.util

import com.zto.bigdata.spark.common.bean.TimeCost
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger

/**
  * String 相关的工具类
  *
  * @author ChengLong 2018-11-1 09:56:33
  */
class LogUtils(logger: Logger) {
  var timeCost: TimeCost = _

  /**
    * 开始记录日志
    */
  def mark: Unit = {
    this.timeCost = TimeCost.build()
  }

  /**
    * 记录日志
    *
    * @param sink
    * 数据的目标源
    * @param action
    * 执行的动作
    * @param msg
    * 错误信息
    */
  def logger(sink: String, action: String = "timecost", msg: String = "success"): Unit = {
    if (this.timeCost == null) this.mark
    this.timeCost.info(sink, action, msg)
    if (StringUtils.isBlank(msg) || "success".equalsIgnoreCase(msg)) {
      logger.info(this.timeCost.toString)
    } else {
      logger.error(this.timeCost.toString)
    }
  }

  /**
    * debug级别日志包裹
    *
    * @param info  日志内容
    * @param color 显示颜色
    * @return
    */
  def wrapLogDebug(info: String, color: String = GlobalConstants.PS1.BLUE): Unit = {
    if (this.logger != null) {
      this.logger.debug(color + GlobalConstants.LogVal.logInfoSplitStart + info + GlobalConstants.LogVal.logInfoSplitEnd + GlobalConstants.PS1.DEFAULT)
    }
  }

  /**
    * info级别日志包裹
    *
    * @param info  日志内容
    * @param color 显示颜色
    * @return
    */
  def wrapLogInfo(info: String, color: String = GlobalConstants.PS1.GREEN): Unit = {
    if (this.logger != null) {
      this.logger.info(color + GlobalConstants.LogVal.logInfoSplitStart + info + GlobalConstants.LogVal.logInfoSplitEnd + GlobalConstants.PS1.DEFAULT)
    }
  }

  /**
    * warn级别日志包裹
    *
    * @param info 日志内容
    * @return
    */
  def wrapLogWarn(info: String, color: String = GlobalConstants.PS1.PINK): Unit = {
    if (this.logger != null) {
      this.logger.warn(color + GlobalConstants.LogVal.logErrorSplitStart + info + GlobalConstants.LogVal.logErrorSplitEnd + GlobalConstants.PS1.DEFAULT)
    }
  }

  /**
    * error级别日志包裹
    *
    * @param info 日志内容
    * @return
    */
  def wrapLogError(info: String, color: String = GlobalConstants.PS1.RED): Unit = {
    if (this.logger != null) {
      this.logger.error(color + GlobalConstants.LogVal.logErrorSplitStart + info + GlobalConstants.LogVal.logErrorSplitEnd + GlobalConstants.PS1.DEFAULT)
    }
  }

  /**
    * log开始分割
    *
    * @param logger
    */
  def logStart(logger: Logger): Unit = {
    if (this.logger != null) {
      this.logger.info(GlobalConstants.LogVal.logStart, GlobalConstants.PS1.YELLOW)
    }
  }

  /**
    * log结束分割
    *
    * @param logger
    */
  def logEnd(logger: Logger): Unit = {
    if (this.logger != null) {
      this.logger.info(GlobalConstants.LogVal.logEnd, GlobalConstants.PS1.YELLOW)
    }
  }
}
