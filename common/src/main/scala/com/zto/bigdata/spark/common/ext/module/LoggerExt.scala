package com.zto.bigdata.spark.common.ext.module

import com.zto.bigdata.spark.common.bean.TimeCost
import com.zto.bigdata.spark.common.util.GlobalConstants
import org.slf4j.Logger

/**
  * 日志扩展
  *
  * @param logger
  * 日志记录器
  * @author ChengLong 2019-6-12 10:32:38
  */
class LoggerExt(logger: Logger) {

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
  def log(sink: String, action: String = "timecost", msg: String = "success", throwable: Throwable = null): Unit = {
    if (this.timeCost == null) this.mark
    this.timeCost.info(sink, action, msg)
    if (throwable == null) logger.info(this.timeCost.toString) else logger.error(this.timeCost.toString, throwable)
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
