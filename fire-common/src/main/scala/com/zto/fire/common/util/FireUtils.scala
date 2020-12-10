package com.zto.fire.common.util

import com.zto.fire.common.conf.{FireFrameworkConf, FirePS1Conf}
import com.zto.fire.common.util.UnitFormatUtils._
import org.slf4j.{Logger, LoggerFactory}

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
   *
   * @param beginTime
   * @return
   */
  def timecost(beginTime: Long): String = readable(currentTime - beginTime, TimeUnitEnum.MS)

  /**
   * 用于统计指定代码块执行的耗时时间
   *
   * @param msg
   * 用于描述当前代码块的用户
   * @param logger
   * 日志记录器
   * @param block
   * try的具体逻辑
   */
  def timecost[T](msg: String, logger: Logger = this.logger)(block: => T): T = {
    val startTime = this.currentTime
    val retVal = block
    logger.info(s"执行${msg}耗时：${timecost(startTime)}")
    retVal
  }
}
