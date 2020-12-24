package com.zto.fire.core.rest

import spark.{Request, Response}

/**
  * 用于封装rest的相关信息
  *
  * @param method
  * rest的提交方式：GET/POST/PUT/DELETE等
  * @param path
  * rest服务地址
  * @author ChengLong 2019-3-16 09:58:06
  */
private[fire] case class RestCase(method: String, path: String, fun: (Request, Response) => AnyRef)
