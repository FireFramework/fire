/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
