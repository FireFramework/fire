package com.zto.bigdata.spark.common.util

import org.slf4j.Logger

/**
  * String 相关的工具类
  *
  * @author ChengLong 2018-11-1 09:56:33
  */
class LogUtils(logger: Logger) {

  /**
    * debug级别日志包裹
    *
    * @param info  日志内容
    * @param color 显示颜色
    * @return
    */
  def wrapLogDebug(info: String, color: String = GlobalConstants.Color.BLUE): Unit = {
    if (this.logger != null) {
      this.logger.debug(color + GlobalConstants.logVal.logInfoSplitStart + info + GlobalConstants.logVal.logInfoSplitEnd + GlobalConstants.Color.DEFAULT)
    }
  }

  /**
    * info级别日志包裹
    *
    * @param info  日志内容
    * @param color 显示颜色
    * @return
    */
  def wrapLogInfo(info: String, color: String = GlobalConstants.Color.GREEN): Unit = {
    if (this.logger != null) {
      this.logger.info(color + GlobalConstants.logVal.logInfoSplitStart + info + GlobalConstants.logVal.logInfoSplitEnd + GlobalConstants.Color.DEFAULT)
    }
  }

  /**
    * warn级别日志包裹
    *
    * @param info 日志内容
    * @return
    */
  def wrapLogWarn(info: String, color: String = GlobalConstants.Color.PINK): Unit = {
    if (this.logger != null) {
      this.logger.error(color + GlobalConstants.logVal.logErrorSplitStart + info + GlobalConstants.logVal.logErrorSplitEnd + GlobalConstants.Color.DEFAULT)
    }
  }

  /**
    * error级别日志包裹
    *
    * @param info 日志内容
    * @return
    */
  def wrapLogError(info: String, color: String = GlobalConstants.Color.RED): Unit = {
    if (this.logger != null) {
      this.logger.error(color + GlobalConstants.logVal.logErrorSplitStart + info + GlobalConstants.logVal.logErrorSplitEnd + GlobalConstants.Color.DEFAULT)
    }
  }

  /**
    * log开始分割
    *
    * @param logger
    */
  def logStart(logger: Logger): Unit = {
    if (this.logger != null) {
      this.logger.info(GlobalConstants.logVal.logStart, GlobalConstants.Color.YELLOW)
    }
  }

  /**
    * log结束分割
    *
    * @param logger
    */
  def logEnd(logger: Logger): Unit = {
    if (this.logger != null) {
      this.logger.info(GlobalConstants.logVal.logEnd, GlobalConstants.Color.YELLOW)
    }
  }
}
