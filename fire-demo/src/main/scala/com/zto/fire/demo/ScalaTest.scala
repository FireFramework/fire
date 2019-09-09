package com.zto.fire.demo

/**
  * 用于测试scala代码
 *
 * @author ChengLong 2019-9-4 13:39:16
  */
object ScalaTest {

  def main(args: Array[String]): Unit = {
    print(this.buildMultiTimerKey("batch", "jdbc", "main", "insert", "t_user", "INFO"))
  }

  def buildMultiTimerKey(cluster: String, module: String, method: String, action: String, sink: String = "", level: String = "INFO", isFire: Boolean = true): String = {
    s"""{"cluster":"$cluster","module":"$module","method":"$method","action":"$action","sink":"$sink","level":"$level","isFire":$isFire,"jobClass":"com.zto.fire.Test"}"""
  }
}
