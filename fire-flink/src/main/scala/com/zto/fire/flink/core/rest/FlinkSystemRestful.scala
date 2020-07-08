package com.zto.fire.flink.core.rest

import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.enu.{ErrorCode, RequestMethod}
import com.zto.fire.common.util._
import com.zto.fire.core.ext.SparkExt._
import com.zto.fire.core.rest.{RestCase, SystemRestful}
import com.zto.fire.flink.core.BaseFlink
import org.apache.commons.lang3.StringUtils
import org.apache.spark.Logging
import spark._

/**
 * 系统预定义的restful服务，为Flink计算引擎提供接口服务
 *
 * @author ChengLong 2020年4月2日 13:50:01
 */
private[fire] class FlinkSystemRestful(val baseFlink: BaseFlink) extends SystemRestful(baseFlink) with Logging {

  /**
   * 注册Flink引擎restful接口
   */
  override protected def register: Unit = {
    this.baseFlink.restfulRegister
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/flink/kill", kill))
      .addRest(RestCase(RequestMethod.GET.toString, s"/system/flink/dataSource", dataSource))
  }

  /**
   * kill 当前 Flink 任务
   */
  @Rest("/system/flink/kill")
  def kill(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg
    val json = request.body
    try {
      // 参数校验与参数获取
      this.baseFlink.shutdown()
      // ProcessUtil.executeCmds(s"yarn application -kill ${this.baseFlink.applicationId}", s"kill -9 ${SystemInfoUtils.getPid}")
      this.logFire(s"[kill] kill任务成功：json=$json", this.module)
      msg.buildSuccess("任务停止成功", ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[kill] 执行kill任务失败：json=$json", this.module, throwable = e)
        msg.buildError("执行kill任务失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }


  /**
   * 获取driver所在服务器的负载信息
   */
  @Rest("/system/loadInfo")
  def loadInfo(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      msg.buildSuccess(SystemInfoUtils.getSystemLoadInfo, ErrorCode.SUCCESS.toString)
    } catch {
      case e: Exception => {
        this.logFire(s"[loadInfo] 获取driver所在主机负载信息失败：json=$json", this.module, throwable = e)
        msg.buildError("获取driver所在主机负载信息失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }

  /**
   * 用于执行sql语句
   */
  @Rest(value = "/system/sql", method = "post")
  def sql(request: Request, response: Response): AnyRef = {
    this.mark
    val msg = new ResultMsg
    val json = request.body
    try {
      // 参数校验与参数获取
      val sql = JSONUtils.getValue(json, "sql", "")

      // sql合法性检查
      if (StringUtils.isBlank(sql) || !sql.toLowerCase.trim.startsWith("select ")) {
        this.logFire(s"[sql] sql不合法，在线调试功能只支持查询操作：json=$json", this.module)
        return msg.buildError(s"sql不合法，在线调试功能只支持查询操作", ErrorCode.ERROR)
      }

      if (this.baseFlink == null || this.baseFlink == null) {
        this.logFire(s"[sql] 系统正在初始化，请稍后再试：json=$json", this.module)
        return "系统正在初始化，请稍后再试"
      }

      /*val sqlResult = this.baseFlink.flink.sql(sql.replace("memory.", "")).limit(1000).showString()
      this.logFire(s"成功执行以下查询：${sql}\n执行结果如下：\n" + sqlResult, this.module)
      msg.buildSuccess(sqlResult, ErrorCode.SUCCESS.toString)*/
      ""
    } catch {
      case e: Exception => {
        this.logFire(s"[sql] 执行用户sql失败：json=$json", this.module, throwable = e)
        msg.buildError("执行用户sql失败，异常堆栈：" + StackTraceUtils.stackTraceInfo(e), ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }
}
