package com.zto.fire.core.rest

import com.zto.fire.common.anno.Rest
import com.zto.fire.common.bean.rest.ResultMsg
import com.zto.fire.common.data.DataPool
import com.zto.fire.common.enu.ErrorCode
import com.zto.fire.core.BaseFire
import org.slf4j.LoggerFactory
import spark.{Request, Response}

/**
 * 系统预定义的restful服务抽象
 *
 * @author ChengLong 2020年4月2日 13:58:08
 */
protected[fire] abstract class SystemRestful(engine: BaseFire) {
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)
  this.register

  /**
   * 注册接口
   */
  protected def register: Unit

  /**
   * 获取当前任务所使用到的数据源信息
   *
   * @return
   * 数据源列表
   */
  @Rest("/system/dataSource")
  protected def dataSource(request: Request, response: Response): AnyRef = {
    val msg = new ResultMsg
    try {
      val dataSource = DataPool.getDatasource
      this.logger.info(s"[DataSource] 获取数据源列表成功：counter=$dataSource")
      msg.buildSuccess(dataSource, "获取数据源列表成功")
    } catch {
      case e => {
        this.logger.error(s"[log] 获取数据源列表失败", e)
        msg.buildError("获取数据源列表失败", ErrorCode.ERROR)
      }
    } finally {
      msg.toString
    }
  }
}
