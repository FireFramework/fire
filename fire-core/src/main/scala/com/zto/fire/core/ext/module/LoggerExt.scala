package com.zto.fire.core.ext.module

import com.zto.fire.common.bean.TimeCost
import com.zto.fire.common.util.GlobalConstants
import org.apache.spark.Logging

/**
  * 日志扩展
  *
  * @param logger
  * 日志记录器
  * @author ChengLong 2019-6-12 10:32:38
  */
class LoggerExt(logger: Logging) extends Logging {
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
    *                  数据的目标源
    * @param action
    *                  执行的动作
    * @param msg
    *                  错误信息
    * @param throwable 异常对象
    * @param sdk
    *                  用于区分系统埋点日志和用户日志，默认为用户日志
    */
  def log(sink: String, action: String = "timecost", msg: String = "success", throwable: Throwable = null, sdk: Boolean = false): Unit = {
    if (this.timeCost == null) this.mark
    this.timeCost.info(sink, action, msg, sdk)
    if (throwable == null) this.logInfo(this.timeCost.toString) else this.logError(this.timeCost.toString, throwable)
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
      this.logDebug(color + GlobalConstants.LogVal.logInfoSplitStart + info + GlobalConstants.LogVal.logInfoSplitEnd + GlobalConstants.PS1.DEFAULT)
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
      this.logInfo(color + GlobalConstants.LogVal.logInfoSplitStart + info + GlobalConstants.LogVal.logInfoSplitEnd + GlobalConstants.PS1.DEFAULT)
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
      this.logWarning(color + GlobalConstants.LogVal.logErrorSplitStart + info + GlobalConstants.LogVal.logErrorSplitEnd + GlobalConstants.PS1.DEFAULT)
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
      this.logError(color + GlobalConstants.LogVal.logErrorSplitStart + info + GlobalConstants.LogVal.logErrorSplitEnd + GlobalConstants.PS1.DEFAULT)
    }
  }

}
