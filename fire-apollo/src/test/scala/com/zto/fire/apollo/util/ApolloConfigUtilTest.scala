package com.zto.fire.apollo.util

object ApolloConfigUtilTest {

  def main(args: Array[String]): Unit = {

    //使用说明：默认使用读取dev环境的配置
    System.setProperty(ApolloConstant.APOLLO_ENV, "dev")

    //可以通过如下参数直接设置属性值，否则根据 env环境配置读取 apollo的值
    //System.setProperty("app.id", "fire1")
    //System.setProperty("apollo.meta", "http://apollo.meta.dev.ztosys.com")

    println(ApolloConfigUtil.getProp)
    println(ApolloConfigUtil.getInt("test"))

  }

}
