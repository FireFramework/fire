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

package com.zto.fire.common.util

import com.zto.fire.common.bean.config.ConfigurationParam
import com.zto.fire.common.conf.FireFrameworkConf
import com.zto.fire.common.enu.ConfigureLevel
import com.zto.fire.predef._
import org.apache.commons.httpclient.Header
import org.apache.commons.lang3.StringUtils

/**
 * 配置中心管理器，用于读取配置中心中的配置信息
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2021-03-12 13:35
 */
private[fire] object ConfigurationCenterManager extends Serializable with Logging {
  private lazy val configCenterProperties: JMap[ConfigureLevel, JMap[String, String]] = new JHashMap[ConfigureLevel, JMap[String, String]]

  /**
   * 构建配置中心请求参数
   *
   * @param className
   * 当前任务主类名
   */
  private[this] def buildRequestParam(className: String): String = {
    val rest = FireFrameworkConf.fireRestUrl
    if (StringUtils.isBlank(rest)) logWarning("Fire Rest Server 地址为空，将无法完成注册")
    s"""
       |{ "url": "$rest", "fireVersion": "${FireFrameworkConf.fireVersion}", "taskId": "${getFireAppId}"}
      """.stripMargin
  }

  /**
   * 从环境变量中获取任务在实时平台中的唯一id标识
   *
   * tips：
   *  1. 通用方式：提交脚本中 export fire_config_center_app_id=101
   *  2. flink任务：-D fire.config_center.app.id=101
   */
  private[this] def getFireAppId: String = {
    var appId = FireFrameworkConf.configCenterAppId

    if (isEmpty(appId)) {
      appId = System.getProperty(FireFrameworkConf.FIRE_CONFIG_CENTER_APP_ID.replace(".", "_"))
    }

    if (isEmpty(appId)) {
      appId = System.getenv(FireFrameworkConf.FIRE_CONFIG_CENTER_APP_ID.replace(".", "_"))
    }

    PropUtils.setProperty(FireFrameworkConf.FIRE_CONFIG_CENTER_APP_ID, appId)

    appId
  }

  /**
   * 通过参数调用指定的接口
   */
  private[this] def invoke(url: String, param: String): String = {
    logInfo(s"开始调用接口：$url,参数为：$param")
    try {
      HttpClientUtils.doPost(url, param, new Header(FireFrameworkConf.configCenterZdpHeaderKey, FireFrameworkConf.configCenterZdpHeaderValue))
    } catch {
      case _: Throwable => logError("调用配置中心接口失败，开始尝试调用测试环境配置中心接口。")
        ""
    }
  }

  /**
   * 调用外部配置中心接口获取配合信息
   */
  def invokeConfigCenter(className: String): JMap[ConfigureLevel, JMap[String, String]] = {
    if (!FireFrameworkConf.configCenterEnable || (FireUtils.isLocalRunMode && !FireFrameworkConf.configCenterLocalEnable)) return this.configCenterProperties
    val param = buildRequestParam(className)
    // 尝试从生产环境配置中心获取参数列表
    var json = this.invoke(FireFrameworkConf.configCenterProdAddress, param)
    // 如果生产环境接口调用失败，可能存在网络隔离，则从测试环境配置中心获取参数列表
    if (isEmpty(json)) json = this.invoke(FireFrameworkConf.configCenterTestAddress, param)
    if (FireFrameworkConf.fireConfShow) logInfo(s"成功获取配置中心配置信息：$json")

    if (isEmpty(json)) {
      // 考虑到任务的重要配置可能存放在配置中心，在接口不通的情况下发布任务存在风险，因此会强制任务退出
      logError("配置中心注册接口不可用导致任务发布失败。如仍需紧急发布，请确保任务配置与配置中心保存一直，并在common.properties中添加以下参数：fire.config_center.enable=false")
      FireUtils.exitError
    } else {
      val param = JSONUtils.parseObject[ConfigurationParam](json)
      if (param.isStatus && noEmpty(param, param.getResult) ) {
        this.configCenterProperties.putAll(param.getResult)
        logInfo("配置中心参数已生效")
      } else{
        logError(s"配置中心接口未生效导致任务发布失败。请直接联系平台管理人员确认。 接口返回为：$json")
        FireUtils.exitError
      }
    }

    this.configCenterProperties
  }

  /**
   * 对jdbc配置的密文密码进行RSA解密
   * @param key
   * 配置的key
   * @param value
   * db.jdbc.password对应的密文
   * @return
   * password明文
   */
  private[fire] def jdbcPasswordDecrypt(key: String, value: String): String = {
    val jdbcPasswordConfKey = "db.jdbc.password"
    // 不解析非JDBC password配置
    if (!key.contains(jdbcPasswordConfKey)) {
      return value
    }

    // 从提交命令所在机器环境中获取私钥信息
    val privateKeyTest = FireFrameworkConf.encryptPrivateKeyTest
    val privateKeyProd = FireFrameworkConf.encryptPrivateKeyProd

    if (isEmpty(privateKeyTest, privateKeyProd)) {
      logWarning("未加载到JDBC密码解密私钥信息，密码信息将直接下发到connector！")
      return value
    }

    // 尝试基于RSA算法对password进行解密，首先基于测试环境的私钥进行解密
    var password = EncryptUtils.rsaDecrypt(value, privateKeyTest)
    if (isEmpty(password)) {
      // 若解密失败，则调用生产密钥进行解密
      password = EncryptUtils.rsaDecrypt(value, privateKeyProd)
    }

    if (noEmpty(password)) {
      logInfo(s"JDBC密文密码解密成功：key=$key")
      password
    } else {
      value
    }
  }
}
