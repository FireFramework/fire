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

package com.zto.fire.examples.flink.sql

import com.zto.fire._
import com.zto.fire.common.util._
import com.zto.fire.core.anno.lifecycle.{Before, Process}
import com.zto.fire.flink.AbstractFlinkStreaming

/**
 * Flink SQL执行类
 *
 * @author ChengLong 2022-11-03 21:47:45
 * @since 2.3.2
 */
class FlinkSQL extends AbstractFlinkStreaming {
  private var sqlStatement = ""

  /**
   * 解析命令行选型
   */
  @Before
  private def options: Unit = {
    // -h：命令选项帮助
    if (this.parameter.has("h") || this.parameter.has("help")) {
      println(
        s"""
          |Usage: ${this.appName} [options]
          |
          |Options:
          |   --help, -h                       Show this help message and exit.
          |   -e <quoted-query-string>         SQL from command line
          |   -f <filename>                    SQL from files
          |   --conf, -D {PROP=VALUE}          Arbitrary configuration property
          |   --show-conf                      Show all configuration property
          |   --info, -i                       Environmental Information
          |""".stripMargin)
      FireUtils.exitNormal
    }

    // --info, -i：打印环境信息
    if (this.parameter.has("info") || this.parameter.has("i")) {
      println(s"fire version: ${FireUtils.fireVersion}  engine version: ${FireUtils.engineVersion}")
      FireUtils.exitNormal
    }

    // --conf：设置配置参数
    if (this.parameter.has("conf") || this.parameter.has("D")) {
      val conf = this.parameter.get("conf")
      val jsonConf = if (noEmpty(conf)) conf else this.parameter.get("D")
      if (JSONUtils.isJson(jsonConf, false)) {
        val map = JSONUtils.parseObject[JMap[String, String]](jsonConf)
        PropUtils.setProperties(map)
      }
    }

    // --show-conf：打印所有配置信息
    if (this.parameter.has("show-conf")) {
      PropUtils.show(false)
      FireUtils.exitNormal
    }

    // -e: 执行sql语句
    if (this.parameter.has("e")) {
      this.sqlStatement = this.parameter.get("e")
      logInfo("SQL语句：" + this.sqlStatement)
    }

    // -f：指定SQL文件
    if (this.parameter.has("f")) {
      this.sqlStatement = HDFSUtils.readTextFile(this.parameter.get("f"))
      logInfo("SQL语句：" + this.sqlStatement)
    }

    // -c：SQL语法校验
    if (this.parameter.has("c")) {
      val retVal = this.sqlValidate(this.parameter.get("c"))
      if (retVal.isFailure) {
        logError("SQL语法错误：" + ExceptionBus.stackTrace(retVal.failed.get))
        FireUtils.exitError
      } else {
        logInfo("SQL语法校验成功")
        FireUtils.exitNormal
      }
    }
  }

  /**
   * 执行SQL语句
   */
  @Process
  def executeSql: Unit = sql(this.sqlStatement)
}

object FlinkSQL {
  def main(args: Array[JString]): Unit = {
    val sql = new FlinkSQL
    sql.init(null, args)
  }
}