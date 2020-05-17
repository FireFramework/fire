package com.zto.fire.core.util

import java.util.concurrent.atomic.AtomicInteger

import com.zto.fire.common.util.{DateFormatUtils, GlobalConstants}

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
}
