package com.zto.fire.core.ext.module

import com.zto.fire.common.bean.BaseLogging
import com.zto.fire.common.util.GlobalConstants
import com.zto.fire.core.util.SparkUtils
import org.apache.spark.Logging

/**
  * 日志扩展
  *
  * @param logger
  * 日志记录器
  * @author ChengLong 2019-6-12 10:32:38
  */
class LoggerExt(logger: Logging) extends BaseLogging with Logging {

  /**
    * 初始化日志记录器
    */
  protected[fire] override def initLogging(className: String): Unit = super.initLogging(className)

  /**
    * 开始记录日志
    */
  override def mark: Unit = {
    super.mark
  }

  /**
    * 用户日志记录器
    *
    * @param msg
    * 日志内容
    * @param throwable
    * 异常信息
    */
  def log(msg: String, throwable: Throwable = null): Unit = {
    super.log(msg, null, null, throwable, false)
  }

  /**
    * fire框架内部日志记录器
    *
    * @param msg
    * @param module
    * @param io
    * @param throwable
    */
  protected[fire] def logFire(msg: String, module: String = null, io: Integer = null, throwable: Throwable = null): Unit = {
    super.log(msg, module, io, throwable)
  }


  /**
    * debug级别日志包裹
    *
    * @param info  日志内容
    * @param color 显示颜色
    * @return
    */
  private[fire] def wrapLogDebug(info: String, color: String = GlobalConstants.PS1.BLUE): Unit = {
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
  private[fire] def wrapLogInfo(info: String, color: String = GlobalConstants.PS1.GREEN): Unit = {
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
  private[fire] def wrapLogWarn(info: String, color: String = GlobalConstants.PS1.PINK): Unit = {
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
  private[fire] def wrapLogError(info: String, color: String = GlobalConstants.PS1.RED): Unit = {
    if (this.logger != null) {
      this.logError(color + GlobalConstants.LogVal.logErrorSplitStart + info + GlobalConstants.LogVal.logErrorSplitEnd + GlobalConstants.PS1.DEFAULT)
    }
  }

}
