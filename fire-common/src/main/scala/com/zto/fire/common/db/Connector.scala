package com.zto.fire.common.db

import java.util.concurrent.ConcurrentHashMap

/**
 * 数据库操作公共父类
 *
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-27 10:32
 */
private[fire] trait Connector

/**
 * 用于根据指定的keyNum创建不同的数据库连接实例
 */
private[fire] class ConnectorFactory[T] {
  private[fire] lazy val instanceMap = new ConcurrentHashMap[Int, T]()

  /**
   * 根据指定的keyNum返回单例的HBaseOper实例
   */
  def getInstance(keyNum: Int = 1): T = this.instanceMap.get(keyNum)
}