package com.zto.fire.common.db.v2

import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory

/**
 * 数据库操作公共父类
 *
 * @param conf
 * 代码级别的配置信息，允许为空，配置文件会覆盖相同配置项，也就是说配置文件拥有着跟高的优先级
 * @param keyNum
 * 用于区分连接不同的数据源，不同配置源对应不同的Oper实例
 * @author ChengLong
 * @since 1.1.2
 * @create 2020-11-27 10:32
 */
private[fire] abstract class DBBaseOper(conf: Any = null, keyNum: Int = 1) {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
}

/**
 * 用于根据指定的keyNum创建不同的数据库连接实例
 */
private[fire] class DBBaseOperFactory {
  private[fire] lazy val instanceMap = new ConcurrentHashMap[Int, DBBaseOper]()

  /**
   * 根据指定的keyNum返回单例的HBaseOper实例
   */
  def getInstance(keyNum: Int = 1): DBBaseOper = this.instanceMap.get(keyNum)
}