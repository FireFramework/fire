package com.zto.fire.common.util

import java.io.FileReader

import com.zto.fire.common.conf.FirePS1Conf
import org.apache.maven.model.io.xpp3.MavenXpp3Reader
import org.slf4j.LoggerFactory

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
   * 判断是否为spark引擎
   */
  def isSparkEngine: Boolean = "spark".equals(PropUtils.engine)

  /**
   * 判断是否为flink引擎
   */
  def isFlinkEngine: Boolean = "flink".equals(PropUtils.engine)

  /**
   * 获取fire版本号
   */
  def fireVersion: String = {
    val reader = new MavenXpp3Reader
    val model = reader.read(new FileReader("pom.xml"))
    model.getVersion
  }

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
          |""".stripMargin.replace("version", s"version ${FirePS1Conf.PINK + this.fireVersion}")

      this.logger.warn(FirePS1Conf.GREEN + info + FirePS1Conf.DEFAULT)
      this.isSplash = true
    }
  }
}
