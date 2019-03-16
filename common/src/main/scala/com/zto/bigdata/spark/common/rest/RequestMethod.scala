package com.zto.bigdata.spark.common.rest

/**
  * 定义http请求的方式枚举
  *
  * @author ChengLong 2019-3-16 10:27:11
  */
object RequestMethod extends Enumeration {
  type RequestMethod = Value

  val GET = Value("get")
  val POST = Value("post")
  val DELETE = Value("delete")
  val PUT = Value("put")
}
