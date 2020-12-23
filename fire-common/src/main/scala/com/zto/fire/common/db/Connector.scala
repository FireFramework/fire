package com.zto.fire.common.db

import java.util.concurrent.ConcurrentHashMap

import com.zto.fire.common.util.ShutdownHookManager
import org.slf4j.{Logger, LoggerFactory}

/**
 * connector父接口，约定了open与close方法，子类需要根据具体
 * 情况覆盖这两个方法。这两个方法不需要子类主动调用，会被自动调用
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-11-27 10:32
 */
private[fire] trait Connector {
  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
  this.open()
  this.hook()

  /**
   * 用于注册释放资源
   */
  private def hook(): Unit = {
    ShutdownHookManager.addShutdownHook() { () => {
      this.close()
      logger.info("release connector successfully.")
    }
    }
  }

  /**
   * connector资源初始化
   */
  protected def open(): Unit

  /**
   * connector资源释放
   */
  protected def close(): Unit
}

/**
 * 用于根据指定的keyNum创建不同的数据库连接实例
 */
private[fire] abstract class ConnectorFactory[T] {
  private[fire] lazy val instanceMap = new ConcurrentHashMap[Int, T]()
  protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 约定创建connector子类实例的方法
   */
  protected def create(conf: Any = null, keyNum: Int = 1): T

  /**
   * 根据指定的keyNum返回单例的HBaseOper实例
   */
  def getInstance(keyNum: Int = 1): T = this.instanceMap.get(keyNum)

  /**
   * 创建指定集群标识的connector对象实例
   */
  def apply(conf: Any = null, keyNum: Int = 1): T = {
    if (!this.instanceMap.containsKey(keyNum)) {
      this.instanceMap.put(keyNum, this.create(conf, keyNum))
    }
    this.instanceMap.get(keyNum)
  }
}